package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	webapi "github.com/bruin-data/bruin/internal/web/api"
	"github.com/bruin-data/bruin/internal/web/events"
	"github.com/bruin-data/bruin/internal/web/freshness"
	webhttpapi "github.com/bruin-data/bruin/internal/web/httpapi"
	webmodel "github.com/bruin-data/bruin/internal/web/model"
	"github.com/bruin-data/bruin/internal/web/service"
	"github.com/bruin-data/bruin/internal/web/sqlintelligence"
	webstatic "github.com/bruin-data/bruin/internal/web/static"
	"github.com/bruin-data/bruin/internal/web/watch"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/telemetry"
	webui "github.com/bruin-data/bruin/web"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

type webAsset struct {
	ID                  string            `json:"id"`
	Name                string            `json:"name"`
	Type                string            `json:"type"`
	Path                string            `json:"path"`
	Content             string            `json:"content"`
	Upstreams           []string          `json:"upstreams"`
	Parameters          map[string]string `json:"parameters,omitempty"`
	Meta                map[string]string `json:"meta,omitempty"`
	Columns             []webColumn       `json:"columns,omitempty"`
	Connection          string            `json:"connection,omitempty"`
	MaterializationType string            `json:"materialization_type,omitempty"`
	IsMaterialized      bool              `json:"is_materialized"`
	MaterializedAs      string            `json:"materialized_as,omitempty"`
	RowCount            *int64            `json:"row_count,omitempty"`
}

type webColumnCheck struct {
	Name        string `json:"name"`
	Value       any    `json:"value,omitempty"`
	Blocking    *bool  `json:"blocking,omitempty"`
	Description string `json:"description,omitempty"`
}

type webColumn struct {
	Name          string            `json:"name"`
	Type          string            `json:"type,omitempty"`
	Description   string            `json:"description,omitempty"`
	Tags          []string          `json:"tags,omitempty"`
	PrimaryKey    bool              `json:"primary_key,omitempty"`
	UpdateOnMerge bool              `json:"update_on_merge,omitempty"`
	MergeSQL      string            `json:"merge_sql,omitempty"`
	Nullable      *bool             `json:"nullable,omitempty"`
	Owner         string            `json:"owner,omitempty"`
	Domains       []string          `json:"domains,omitempty"`
	Meta          map[string]string `json:"meta,omitempty"`
	Checks        []webColumnCheck  `json:"checks,omitempty"`
}

type apiError = webhttpapi.APIError
type formatSQLAssetResponse = webhttpapi.FormatSQLAssetResponse

type ingestrSuggestionItem struct {
	Value  string `json:"value"`
	Kind   string `json:"kind,omitempty"`
	Detail string `json:"detail,omitempty"`
}

type sqlDiscoveryDatabaseResponse struct {
	Status         string   `json:"status"`
	ConnectionName string   `json:"connection_name"`
	ConnectionType string   `json:"connection_type,omitempty"`
	Databases      []string `json:"databases"`
	Error          string   `json:"error,omitempty"`
}

type sqlDiscoveryTableItem struct {
	Name         string `json:"name"`
	ShortName    string `json:"short_name"`
	SchemaName   string `json:"schema_name,omitempty"`
	DatabaseName string `json:"database_name,omitempty"`
}

type sqlDiscoveryTablesResponse struct {
	Status         string                  `json:"status"`
	ConnectionName string                  `json:"connection_name"`
	ConnectionType string                  `json:"connection_type,omitempty"`
	Database       string                  `json:"database"`
	Tables         []sqlDiscoveryTableItem `json:"tables"`
	Error          string                  `json:"error,omitempty"`
}

type sqlDiscoveryTableColumnsResponse struct {
	Status         string      `json:"status"`
	ConnectionName string      `json:"connection_name"`
	Table          string      `json:"table"`
	Columns        []webColumn `json:"columns"`
	RawOutput      string      `json:"raw_output"`
	Command        []string    `json:"command,omitempty"`
	Error          string      `json:"error,omitempty"`
}

type ingestrSuggestionsResponse struct {
	Status         string                  `json:"status"`
	ConnectionType string                  `json:"connection_type,omitempty"`
	Suggestions    []ingestrSuggestionItem `json:"suggestions"`
	Error          string                  `json:"error,omitempty"`
}

type sqlPathSuggestionsResponse struct {
	Status      string                  `json:"status"`
	Suggestions []ingestrSuggestionItem `json:"suggestions"`
	Error       string                  `json:"error,omitempty"`
}

type sqlParseContextRequest struct {
	AssetID string `json:"asset_id"`
	Content string `json:"content"`
	Schema  []struct {
		Name    string `json:"name"`
		Columns []struct {
			Name string `json:"name"`
			Type string `json:"type,omitempty"`
		} `json:"columns"`
	} `json:"schema"`
}

type sqlParseContextRangeResponse struct {
	Start   int `json:"start"`
	End     int `json:"end"`
	Line    int `json:"line"`
	Col     int `json:"col"`
	EndLine int `json:"end_line"`
	EndCol  int `json:"end_col"`
}

type sqlParseContextPartResponse struct {
	Name  string                       `json:"name"`
	Kind  string                       `json:"kind"`
	Range sqlParseContextRangeResponse `json:"range"`
}

type sqlParseContextDiagnosticResponse struct {
	Message  string                        `json:"message"`
	Severity string                        `json:"severity"`
	Range    *sqlParseContextRangeResponse `json:"range,omitempty"`
}

type sqlParseContextTableResponse struct {
	Name         string                        `json:"name"`
	SourceKind   string                        `json:"source_kind,omitempty"`
	ResolvedName string                        `json:"resolved_name,omitempty"`
	Alias        string                        `json:"alias,omitempty"`
	Parts        []sqlParseContextPartResponse `json:"parts"`
	AliasRange   *sqlParseContextRangeResponse `json:"alias_range,omitempty"`
}

type sqlParseContextColumnResponse struct {
	Name          string                        `json:"name"`
	Qualifier     string                        `json:"qualifier,omitempty"`
	ResolvedTable string                        `json:"resolved_table,omitempty"`
	Parts         []sqlParseContextPartResponse `json:"parts"`
}

type sqlParseContextResponse struct {
	Status         string                              `json:"status"`
	AssetID        string                              `json:"asset_id"`
	Dialect        string                              `json:"dialect,omitempty"`
	QueryKind      string                              `json:"query_kind,omitempty"`
	IsSingleSelect bool                                `json:"is_single_select"`
	Tables         []sqlParseContextTableResponse      `json:"tables"`
	Columns        []sqlParseContextColumnResponse     `json:"columns"`
	Diagnostics    []sqlParseContextDiagnosticResponse `json:"diagnostics,omitempty"`
	Errors         []string                            `json:"errors,omitempty"`
	Error          string                              `json:"error,omitempty"`
}

type sqlColumnValuesRequest struct {
	Connection  string `json:"connection"`
	Environment string `json:"environment,omitempty"`
	Query       string `json:"query"`
}

type sqlColumnValuesResponse struct {
	Status string `json:"status"`
	Values []any  `json:"values"`
	Error  string `json:"error,omitempty"`
}

var assetTypeDialectMap = map[pipeline.AssetType]string{
	pipeline.AssetTypeBigqueryQuery:   "bigquery",
	pipeline.AssetTypeSnowflakeQuery:  "snowflake",
	pipeline.AssetTypePostgresQuery:   "postgres",
	pipeline.AssetTypeMySQLQuery:      "mysql",
	pipeline.AssetTypeRedshiftQuery:   "redshift",
	pipeline.AssetTypeAthenaQuery:     "athena",
	pipeline.AssetTypeClickHouse:      "clickhouse",
	pipeline.AssetTypeDatabricksQuery: "databricks",
	pipeline.AssetTypeMsSQLQuery:      "tsql",
	pipeline.AssetTypeSynapseQuery:    "tsql",
	pipeline.AssetTypeDuckDBQuery:     "duckdb",
}

func assetTypeToDialect(assetType pipeline.AssetType) (string, error) {
	dialect, ok := assetTypeDialectMap[assetType]
	if !ok {
		return "", fmt.Errorf("unsupported asset type %s", assetType)
	}

	return dialect, nil
}

type webPipeline struct {
	ID     string     `json:"id"`
	Name   string     `json:"name"`
	Path   string     `json:"path"`
	Assets []webAsset `json:"assets"`
}

type workspaceState struct {
	Pipelines           []webPipeline       `json:"pipelines"`
	Connections         map[string]string   `json:"connections"`
	SelectedEnvironment string              `json:"selected_environment"`
	Errors              []string            `json:"errors"`
	UpdatedAt           time.Time           `json:"updated_at"`
	Metadata            map[string][]string `json:"metadata"`
	Revision            int64               `json:"revision,omitempty"`
}

type workspaceEvent struct {
	Type            string         `json:"type"`
	Path            string         `json:"path,omitempty"`
	Workspace       workspaceState `json:"workspace"`
	Lite            bool           `json:"lite,omitempty"`
	ChangedAssetIDs []string       `json:"changed_asset_ids,omitempty"`
}

type workspaceConfigFieldDef = service.WorkspaceConfigFieldDef

type webServer struct {
	workspaceRoot string
	staticDir     string
	staticHandler http.Handler
	watchMode     string
	watchPoll     time.Duration
	workspaceSvc  *service.WorkspaceService
	configSvc     *service.ConfigService
	pipelineSvc   *service.PipelineService
	executionSvc  *service.ExecutionService
	assetSvc      *service.AssetService
	sqlSvc        *service.SQLService

	stateMu  sync.RWMutex
	state    workspaceState
	revision atomic.Int64

	hub       *events.Hub
	runner    service.Runner
	freshness *freshness.Tracker

	duckDBOpsMu sync.Mutex
	duckDBOps   map[string]*sync.Mutex

	// recentServerWrites tracks paths recently written by API handlers or
	// patch timers. The filesystem watcher suppresses events for these paths
	// to avoid duplicate notifications (the handler already emits its own event).
	recentServerWritesMu sync.Mutex
	recentServerWrites   map[string]time.Time
}

func Web() *cli.Command {
	return &cli.Command{
		Name:      "web",
		Usage:     "start Bruin Web UI server",
		ArgsUsage: "[workspace root]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "host",
				Value: "127.0.0.1",
				Usage: "host interface to bind",
			},
			&cli.IntFlag{
				Name:  "port",
				Value: 8080,
				Usage: "HTTP port",
			},
			&cli.StringFlag{
				Name:  "static-dir",
				Value: "web/dist",
				Usage: "optional override directory for static web assets",
			},
			&cli.StringFlag{
				Name:  "watch-mode",
				Value: "auto",
				Usage: "workspace watcher mode: auto, fsnotify, or poll",
			},
			&cli.DurationFlag{
				Name:  "watch-poll-interval",
				Value: 2 * time.Second,
				Usage: "poll interval used when watch-mode is poll or auto",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			root := c.Args().Get(0)
			if root == "" {
				root = "."
			}

			absRoot, err := filepath.Abs(root)
			if err != nil {
				return fmt.Errorf("failed to resolve workspace root: %w", err)
			}

			staticDir := c.String("static-dir")
			if !filepath.IsAbs(staticDir) {
				staticDir = filepath.Join(absRoot, staticDir)
			}

			watchMode := strings.ToLower(strings.TrimSpace(c.String("watch-mode")))
			if watchMode == "" {
				watchMode = "auto"
			}
			if watchMode != "auto" && watchMode != "fsnotify" && watchMode != "poll" {
				return fmt.Errorf("invalid watch-mode %q, expected one of: auto, fsnotify, poll", watchMode)
			}

			watchPoll := c.Duration("watch-poll-interval")
			if watchPoll <= 0 {
				return fmt.Errorf("watch-poll-interval must be greater than zero")
			}

			server := &webServer{
				workspaceRoot:      absRoot,
				staticDir:          staticDir,
				watchMode:          watchMode,
				watchPoll:          watchPoll,
				workspaceSvc:       service.NewWorkspaceService(absRoot, resolveConfigFilePath(absRoot)),
				configSvc:          service.NewConfigService(absRoot, resolveConfigFilePath(absRoot)),
				pipelineSvc:        service.NewPipelineService(absRoot),
				hub:                events.NewDebouncedHub(150 * time.Millisecond),
				runner:             service.NewRunner(absRoot),
				freshness:          freshness.New(),
				duckDBOps:          make(map[string]*sync.Mutex),
				recentServerWrites: make(map[string]time.Time),
			}

			server.executionSvc = service.NewExecutionService(service.ExecutionDependencies{
				WorkspaceRoot:         absRoot,
				ConfigPath:            resolveConfigFilePath(absRoot),
				Runner:                server.runner,
				ResolveAssetByID:      server.resolveAssetByID,
				ResolveAssetNameByID:  server.findAssetNameByID,
				FindInspectIDs:        server.findMaterializationInspectIDs,
				RecordMaterialization: server.freshness.RecordMaterialization,
				CurrentPipelines: func() []service.PipelineView {
					state := server.currentState()
					pipelines := make([]service.PipelineView, 0, len(state.Pipelines))
					for _, pipeline := range state.Pipelines {
						assets := make([]service.AssetView, 0, len(pipeline.Assets))
						for _, asset := range pipeline.Assets {
							assets = append(assets, service.AssetView{ID: asset.ID, Name: asset.Name})
						}
						pipelines = append(pipelines, service.PipelineView{ID: pipeline.ID, Assets: assets})
					}
					return pipelines
				},
				DuckDBLock: func(lockKey string) *sync.Mutex {
					return server.getDuckDBOperationMutex(lockKey)
				},
				ParseQueryOutput:   service.ParseQueryJSONOutput,
				NewPipelineBuilder: server.newPipelineBuilder,
				FreshnessSnapshot: func() map[string]service.AssetTimestamps {
					items := server.freshness.GetAll()
					result := make(map[string]service.AssetTimestamps, len(items))
					for key, item := range items {
						result[key] = service.AssetTimestamps{
							MaterializedAt:   item.MaterializedAt,
							ContentChangedAt: item.ContentChangedAt,
							LastStatus:       item.MaterializedStatus,
						}
					}
					return result
				},
			})

			server.assetSvc = service.NewAssetService(service.AssetDependencies{
				WorkspaceRoot:                absRoot,
				Runner:                       server.runner,
				ResolveAssetByID:             server.resolveAssetByID,
				DefaultAssetContent:          defaultAssetContent,
				DerivedAssetContent:          defaultDerivedSQLAssetContent,
				EnsurePythonRequirements:     ensurePythonRequirementsFile,
				SuppressWatcher:              server.suppressWatcherFor,
				PushWorkspaceUpdate:          server.pushWorkspaceUpdate,
				PushWorkspaceUpdateImmediate: server.pushWorkspaceUpdateImmediate,
				PushWorkspaceUpdateImmediateWithChangedIDs: server.pushWorkspaceUpdateImmediateWithChangedIDs,
			})

			server.sqlSvc = service.NewSQLService(service.SQLDependencies{
				Runner:               server.runner,
				NewConnectionManager: server.newConnectionManager,
				RunConnectionQuery:   server.executionSvc.RunConnectionQueryForEnvironment,
			})

			embeddedStaticFS, err := webui.DistFS()
			if err != nil {
				embeddedStaticFS = nil
			}

			server.staticHandler, err = webstatic.NewHandler(embeddedStaticFS, staticDir)
			if err != nil {
				return fmt.Errorf("failed to initialize static asset handler: %w", err)
			}

			// Bootstrap materialization timestamps from existing run logs.
			logsDir := filepath.Join(absRoot, "logs")
			if err := server.freshness.LoadFromRunLogs(logsDir); err != nil {
				fmt.Printf("warning: failed to load run logs for freshness tracking: %v\n", err)
			}

			if err := server.refreshWorkspace(ctx); err != nil {
				fmt.Printf("warning: initial workspace parse failed: %v\n", err)
			}

			go watch.New(watch.Config{
				WorkspaceRoot: absRoot,
				Mode:          watchMode,
				PollInterval:  watchPoll,
			}, func(ctx context.Context, eventType, eventPath string) {
				if server.isWatcherSuppressed(eventPath) {
					return
				}
				server.pushWorkspaceUpdate(ctx, eventType, eventPath)
			}).Start(ctx)

			router := chi.NewRouter()
			server.registerRoutes(router)

			address := fmt.Sprintf("%s:%d", c.String("host"), c.Int("port"))
			fmt.Printf("Bruin Web listening on http://%s\n", address)

			httpServer := &http.Server{
				Addr:              address,
				Handler:           router,
				ReadHeaderTimeout: 10 * time.Second,
			}

			go func() {
				<-ctx.Done()
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = httpServer.Shutdown(shutdownCtx)
			}()

			err = httpServer.ListenAndServe()
			if err != nil && err != http.ErrServerClosed {
				return err
			}

			return nil
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}

func (s *webServer) registerRoutes(router chi.Router) {
	webhttpapi.RegisterWorkspaceRoutes(router, &webhttpapi.WorkspaceHandlers{Reader: s})
	webhttpapi.RegisterConfigRoutes(router, &webhttpapi.ConfigHandlers{Service: s.configSvc, Publisher: s})
	webhttpapi.RegisterPipelineRoutes(router, &webhttpapi.PipelineHandlers{Service: s.pipelineSvc, Publisher: s})
	webhttpapi.RegisterExecutionRoutes(router, &webhttpapi.ExecutionAPI{Service: s})
	webhttpapi.RegisterAssetRoutes(router, &webhttpapi.AssetsAPI{Service: s})
	webhttpapi.RegisterAssetColumnRoutes(router, &webhttpapi.AssetColumnsAPI{Service: s})
	webhttpapi.RegisterPipelineExecutionRoutes(router, &webhttpapi.PipelineExecutionAPI{Service: s})
	webhttpapi.RegisterSQLRoutes(router, &webhttpapi.SQLAPI{Service: s})
	router.Get("/api/ingestr/suggestions", s.handleGetIngestrSuggestions)
	router.Get("/api/assets/{assetID}/sql-path-suggestions", s.handleGetSQLPathSuggestions)
	router.Post("/api/sql/parse-context", s.handleSQLParseContext)
	router.Get("/api/assets/freshness", s.handleGetAssetFreshness)
	router.Post("/api/run", s.handleRun)

	router.Get("/*", s.handleStatic)
}

func (s *webServer) currentState() workspaceState {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.state
}

func (s *webServer) setState(state workspaceState) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.state = state
}

func (s *webServer) refreshWorkspace(ctx context.Context) error {
	if err := s.workspaceSvc.Refresh(ctx); err != nil {
		return err
	}

	state := workspaceStateFromModel(s.workspaceSvc.GetState())
	state.Revision = s.revision.Add(1)
	s.setState(state)
	return nil
}

func workspaceStateFromModel(state webmodel.WorkspaceState) workspaceState {
	result := workspaceState{
		Pipelines:           make([]webPipeline, 0, len(state.Pipelines)),
		Connections:         mapsClone(state.Connections),
		SelectedEnvironment: state.SelectedEnvironment,
		Errors:              append([]string(nil), state.Errors...),
		UpdatedAt:           state.UpdatedAt,
		Metadata:            mapSliceClone(state.Metadata),
	}

	for _, pipeline := range state.Pipelines {
		result.Pipelines = append(result.Pipelines, webPipelineFromModel(pipeline))
	}

	return result
}

func webPipelineFromModel(pipeline webmodel.Pipeline) webPipeline {
	result := webPipeline{
		ID:     pipeline.ID,
		Name:   pipeline.Name,
		Path:   pipeline.Path,
		Assets: make([]webAsset, 0, len(pipeline.Assets)),
	}

	for _, asset := range pipeline.Assets {
		result.Assets = append(result.Assets, webAssetFromModel(asset))
	}

	return result
}

func webAssetFromModel(asset webmodel.Asset) webAsset {
	result := webAsset{
		ID:                  asset.ID,
		Name:                asset.Name,
		Type:                asset.Type,
		Path:                asset.Path,
		Content:             asset.Content,
		Upstreams:           append([]string(nil), asset.Upstreams...),
		Parameters:          mapsClone(asset.Parameters),
		Meta:                mapsClone(asset.Meta),
		Columns:             make([]webColumn, 0, len(asset.Columns)),
		Connection:          asset.Connection,
		MaterializationType: asset.MaterializationType,
		IsMaterialized:      asset.IsMaterialized,
		MaterializedAs:      asset.MaterializedAs,
		RowCount:            asset.RowCount,
	}

	for _, column := range asset.Columns {
		result.Columns = append(result.Columns, webColumnFromModel(column))
	}

	return result
}

func webColumnFromModel(column webmodel.Column) webColumn {
	result := webColumn{
		Name:          column.Name,
		Type:          column.Type,
		Description:   column.Description,
		Tags:          append([]string(nil), column.Tags...),
		PrimaryKey:    column.PrimaryKey,
		UpdateOnMerge: column.UpdateOnMerge,
		MergeSQL:      column.MergeSQL,
		Nullable:      column.Nullable,
		Owner:         column.Owner,
		Domains:       append([]string(nil), column.Domains...),
		Meta:          mapsClone(column.Meta),
		Checks:        make([]webColumnCheck, 0, len(column.Checks)),
	}

	for _, check := range column.Checks {
		result.Checks = append(result.Checks, webColumnCheck{
			Name:        check.Name,
			Value:       check.Value,
			Blocking:    check.Blocking,
			Description: check.Description,
		})
	}

	return result
}

func mapsClone(input map[string]string) map[string]string {
	if len(input) == 0 {
		return map[string]string{}
	}

	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func pipelineExecutionStatesToAPI(input []service.PipelineMaterializationState) []webhttpapi.PipelineMaterializationState {
	result := make([]webhttpapi.PipelineMaterializationState, 0, len(input))
	for _, item := range input {
		result = append(result, webhttpapi.PipelineMaterializationState{
			AssetID:         item.AssetID,
			IsMaterialized:  item.IsMaterialized,
			MaterializedAs:  item.MaterializedAs,
			FreshnessStatus: item.FreshnessStatus,
			RowCount:        item.RowCount,
			Connection:      item.Connection,
			DeclaredMatType: item.DeclaredMatType,
		})
	}
	return result
}

func mapSliceClone(input map[string][]string) map[string][]string {
	if len(input) == 0 {
		return map[string][]string{}
	}

	result := make(map[string][]string, len(input))
	for key, values := range input {
		result[key] = append([]string(nil), values...)
	}
	return result
}

func (s *webServer) newPipelineBuilder() *pipeline.Builder {
	osFS := afero.NewOsFs()
	return pipeline.NewBuilder(
		builderConfig,
		pipeline.CreateTaskFromYamlDefinition(osFS),
		pipeline.CreateTaskFromFileComments(osFS),
		osFS,
		DefaultGlossaryReader,
	)
}

func resolveConfigFilePath(workspaceRoot string) string {
	repoRoot, err := git.FindRepoFromPath(workspaceRoot)
	if err == nil && repoRoot != nil && strings.TrimSpace(repoRoot.Path) != "" {
		return filepath.Join(repoRoot.Path, ".bruin.yml")
	}

	return filepath.Join(workspaceRoot, ".bruin.yml")
}

func (s *webServer) resolveConfigFilePath() string {
	return resolveConfigFilePath(s.workspaceRoot)
}

func (s *webServer) ConfigChanged(ctx context.Context, relPath, eventType string) {
	s.suppressWatcherFor(relPath)
	s.pushWorkspaceUpdateImmediate(ctx, eventType, relPath)
}

func (s *webServer) WorkspaceChanged(ctx context.Context, relPath, eventType string) {
	s.suppressWatcherFor(relPath)
	s.pushWorkspaceUpdateImmediate(ctx, eventType, relPath)
}

func (s *webServer) CurrentWorkspace() any {
	return s.currentState()
}

func (s *webServer) CurrentWorkspaceLite() any {
	return webhttpapi.WorkspaceUpdatedEvent{
		Type:      "workspace.updated",
		Workspace: stripAssetContent(s.currentState()),
		Lite:      true,
	}
}

func (s *webServer) SubscribeWorkspaceEvents() chan []byte {
	return s.hub.Subscribe()
}

func (s *webServer) UnsubscribeWorkspaceEvents(ch chan []byte) {
	s.hub.Unsubscribe(ch)
}

func (s *webServer) writeJSON(w http.ResponseWriter, status int, body any) {
	webapi.WriteJSON(w, status, body)
}

type executionInspectResult = webhttpapi.InspectExecutionResult

type executionMaterializeEvent = webhttpapi.MaterializeExecutionEvent

func (s *webServer) InspectAsset(ctx context.Context, assetID, limit, environment string) executionInspectResult {
	return executionInspectResult(s.executionSvc.InspectAsset(ctx, assetID, limit, environment))
}

func (s *webServer) MaterializeAssetStream(ctx context.Context, assetID string, onChunk func([]byte)) executionMaterializeEvent {
	return executionMaterializeEvent(s.executionSvc.MaterializeAssetStream(ctx, assetID, onChunk))
}

func (s *webServer) GetPipelineMaterialization(ctx context.Context, pipelineID string) (webhttpapi.PipelineMaterializationResponse, *apiError) {
	response, err := s.executionSvc.GetPipelineMaterialization(ctx, pipelineID)
	if err != nil {
		message := err.Error()
		switch {
		case strings.Contains(message, "invalid pipeline id"):
			return webhttpapi.PipelineMaterializationResponse{}, &apiError{Status: http.StatusBadRequest, Code: "invalid_pipeline_id", Message: "invalid pipeline id"}
		case strings.Contains(message, "invalid path"):
			return webhttpapi.PipelineMaterializationResponse{}, &apiError{Status: http.StatusBadRequest, Code: "invalid_pipeline_path", Message: message}
		default:
			return webhttpapi.PipelineMaterializationResponse{}, &apiError{Status: http.StatusBadRequest, Code: "pipeline_parse_failed", Message: message}
		}
	}
	return webhttpapi.PipelineMaterializationResponse{PipelineID: response.PipelineID, Assets: pipelineExecutionStatesToAPI(response.Assets)}, nil
}

func (s *webServer) CreateAsset(ctx context.Context, pipelineID string, req webhttpapi.CreateAssetRequest) (map[string]string, *apiError) {
	result, err := s.assetSvc.Create(ctx, pipelineID, service.CreateAssetParams(req))
	if err != nil {
		return nil, &apiError{Status: err.Status, Code: err.Code, Message: err.Message}
	}
	return result, nil
}

type updateAssetColumnsRequest struct {
	Columns []webColumn `json:"columns"`
}

func (s *webServer) UpdateAsset(ctx context.Context, assetID string, req webhttpapi.UpdateAssetRequest) (map[string]string, *apiError) {
	result, err := s.assetSvc.Update(ctx, assetID, service.AssetUpdateRequest{
		Name:                req.Name,
		Type:                req.Type,
		Content:             req.Content,
		MaterializationType: req.MaterializationType,
		Meta:                req.Meta,
	})
	if err != nil {
		return nil, &apiError{Status: err.Status, Code: err.Code, Message: err.Message}
	}
	return result, nil
}

func (s *webServer) FormatSQLAsset(ctx context.Context, assetID string, req webhttpapi.FormatSQLAssetRequest) (formatSQLAssetResponse, *apiError) {
	result, err := s.assetSvc.FormatSQL(ctx, assetID, service.FormatSQLAssetRequest{Content: req.Content})
	if err != nil {
		return formatSQLAssetResponse{}, &apiError{Status: err.Status, Code: err.Code, Message: err.Message}
	}
	return formatSQLAssetResponse(result), nil
}

func replaceAssetNameReferences(content, oldName, newName string) string {
	return service.ReplaceAssetNameReferences(content, oldName, newName)
}

func quoteQualifiedIdentifier(value string) string {
	return service.QuoteQualifiedIdentifier(value)
}

func readStringField(row map[string]any, keys ...string) string {
	return service.ReadStringField(row, keys...)
}

func (s *webServer) FillColumnsFromDB(ctx context.Context, assetID string) (int, map[string]any, *apiError) {
	relAssetPath, _, asset, err := s.resolveAssetByID(ctx, assetID)
	if err != nil {
		return 0, nil, &apiError{Status: http.StatusBadRequest, Code: "asset_resolve_failed", Message: err.Error()}
	}

	assetType := strings.ToLower(string(asset.Type))
	if !strings.Contains(assetType, "sql") && !strings.HasSuffix(strings.ToLower(relAssetPath), ".sql") {
		return 0, nil, &apiError{Status: http.StatusBadRequest, Code: "unsupported_asset_type", Message: "fill-columns-from-db is supported for sql assets only"}
	}

	normalizedPath := filepath.ToSlash(relAssetPath)
	withDot := "./" + strings.TrimPrefix(normalizedPath, "./")
	withoutDot := strings.TrimPrefix(normalizedPath, "./")

	commands := [][]string{
		{"patch", "fill-columns-from-db", withDot},
		{"patch", "fill-columns-from-db", withoutDot},
	}

	type cmdResult struct {
		Command  []string `json:"command"`
		Output   string   `json:"output"`
		ExitCode int      `json:"exit_code"`
		Error    string   `json:"error,omitempty"`
	}

	results := make([]cmdResult, 0, len(commands))
	allSucceeded := true

	for _, args := range commands {
		out, runErr := s.runner.Run(ctx, args)

		result := cmdResult{
			Command:  args,
			Output:   string(out),
			ExitCode: 0,
		}

		if runErr != nil {
			allSucceeded = false
			result.ExitCode = 1
			result.Error = runErr.Error()
		}

		results = append(results, result)
	}

	s.suppressWatcherFor(relAssetPath)
	s.pushWorkspaceUpdateImmediate(ctx, "asset.updated", relAssetPath)

	status := http.StatusOK
	responseStatus := "ok"
	if !allSucceeded {
		status = http.StatusBadRequest
		responseStatus = "error"
	}

	return status, map[string]any{
		"status":  responseStatus,
		"results": results,
	}, nil
}

func (s *webServer) InferAssetColumns(ctx context.Context, assetID string) (int, map[string]any, *apiError) {
	_, parsedPipeline, asset, err := s.resolveAssetByID(ctx, assetID)
	if err != nil {
		return 0, nil, &apiError{Status: http.StatusBadRequest, Code: "asset_resolve_failed", Message: err.Error()}
	}

	cmdArgs, err := buildInferAssetColumnsCommand(parsedPipeline, asset)
	if err != nil {
		return 0, nil, &apiError{Status: http.StatusBadRequest, Code: "infer_columns_command_build_failed", Message: err.Error()}
	}

	output, err := s.runner.Run(ctx, cmdArgs)
	if err != nil {
		return http.StatusBadRequest, map[string]any{
			"status":     "error",
			"columns":    []webColumn{},
			"raw_output": string(output),
			"command":    cmdArgs,
			"error":      err.Error(),
		}, nil
	}

	inferred := inferWebColumnsFromQueryOutput(output)
	return http.StatusOK, map[string]any{
		"status":     "ok",
		"columns":    inferred,
		"raw_output": string(output),
		"command":    cmdArgs,
	}, nil
}

func buildInferAssetColumnsCommand(parsedPipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]string, error) {
	return service.BuildInferAssetColumnsCommand(parsedPipeline, asset)
}

func buildRemoteTableColumnsCommand(connectionName, query, environment string) []string {
	return service.BuildRemoteTableColumnsCommand(connectionName, query, environment)
}

func (s *webServer) UpdateAssetColumns(ctx context.Context, assetID string, columns []any) (map[string]string, *apiError) {
	_, parsedPipeline, asset, err := s.resolveAssetByID(ctx, assetID)
	if err != nil {
		return nil, &apiError{Status: http.StatusBadRequest, Code: "asset_resolve_failed", Message: err.Error()}
	}

	columnBytes, err := json.Marshal(columns)
	if err != nil {
		return nil, &apiError{Status: http.StatusBadRequest, Code: "invalid_request_body", Message: err.Error()}
	}

	var req updateAssetColumnsRequest
	if err := json.Unmarshal(columnBytes, &req.Columns); err != nil {
		return nil, &apiError{Status: http.StatusBadRequest, Code: "invalid_request_body", Message: err.Error()}
	}

	asset.Columns = webColumnsToPipelineColumns(req.Columns)
	err = asset.Persist(afero.NewOsFs(), parsedPipeline)
	if err != nil {
		return nil, &apiError{Status: http.StatusInternalServerError, Code: "asset_persist_failed", Message: err.Error()}
	}

	relAssetPath, decodeErr := decodeID(assetID)
	if decodeErr == nil {
		s.suppressWatcherFor(relAssetPath)
		s.pushWorkspaceUpdateImmediate(ctx, "asset.columns.updated", relAssetPath)
	}

	return map[string]string{"status": "ok"}, nil
}

func (s *webServer) DeleteAsset(ctx context.Context, assetID string) (map[string]string, *apiError) {
	result, err := s.assetSvc.Delete(ctx, assetID)
	if err != nil {
		return nil, &apiError{Status: err.Status, Code: err.Code, Message: err.Message}
	}
	return result, nil
}

func (s *webServer) resolveAssetByID(ctx context.Context, assetID string) (string, *pipeline.Pipeline, *pipeline.Asset, error) {
	return s.workspaceSvc.ResolveAssetByID(ctx, assetID)
}

func (s *webServer) getDuckDBOperationMutex(lockKey string) *sync.Mutex {
	s.duckDBOpsMu.Lock()
	defer s.duckDBOpsMu.Unlock()

	if existing, ok := s.duckDBOps[lockKey]; ok {
		return existing
	}

	mu := &sync.Mutex{}
	s.duckDBOps[lockKey] = mu
	return mu
}

func (s *webServer) handleGetIngestrSuggestions(w http.ResponseWriter, r *http.Request) {
	connectionName := strings.TrimSpace(r.URL.Query().Get("connection"))
	if connectionName == "" {
		webapi.WriteBadRequest(w, "connection_required", "connection query parameter is required")
		return
	}

	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	environment := strings.TrimSpace(r.URL.Query().Get("environment"))

	manager, err := s.newConnectionManager(r.Context(), environment)
	if err != nil {
		webapi.WriteInternalError(w, "connection_manager_failed", err.Error())
		return
	}

	conn := manager.GetConnection(connectionName)
	if conn == nil {
		webapi.WriteBadRequest(w, "connection_not_found", fmt.Sprintf("connection '%s' not found", connectionName))
		return
	}

	connType := strings.TrimSpace(manager.GetConnectionType(connectionName))
	response := ingestrSuggestionsResponse{
		Status:         "ok",
		ConnectionType: connType,
		Suggestions:    []ingestrSuggestionItem{},
	}

	if s3Conn, ok := conn.(interface {
		ListBuckets(ctx context.Context) ([]string, error)
		ListEntries(ctx context.Context, bucketName, prefix string) ([]string, error)
	}); ok {
		items, itemErr := buildS3SuggestionItems(
			r.Context(),
			s3Conn,
			prefix,
			manager.GetConnectionDetails(connectionName),
		)
		if itemErr != nil {
			webapi.WriteBadRequest(w, "ingestr_s3_suggestions_failed", itemErr.Error())
			return
		}
		response.Suggestions = items
		s.writeJSON(w, http.StatusOK, response)
		return
	}

	if fetcherWithSchemas, ok := conn.(interface {
		GetTablesWithSchemas(ctx context.Context, databaseName string) (map[string][]string, error)
	}); ok {
		databaseName := databaseNameForConnectionDetails(manager.GetConnectionDetails(connectionName))
		if databaseName == "" {
			webapi.WriteBadRequest(w, "database_name_missing", fmt.Sprintf("connection '%s' has no database configured", connectionName))
			return
		}

		tables, tableErr := fetcherWithSchemas.GetTablesWithSchemas(r.Context(), databaseName)
		if tableErr != nil {
			webapi.WriteBadRequest(w, "ingestr_table_suggestions_failed", tableErr.Error())
			return
		}

		response.Suggestions = buildSchemaTableSuggestionItems(tables, prefix)
		s.writeJSON(w, http.StatusOK, response)
		return
	}

	if fetcher, ok := conn.(interface {
		GetDatabases(ctx context.Context) ([]string, error)
		GetTables(ctx context.Context, databaseName string) ([]string, error)
	}); ok {
		suggestions, tableErr := buildDuckDBSuggestionItems(r.Context(), fetcher, prefix)
		if tableErr != nil {
			webapi.WriteBadRequest(w, "ingestr_table_suggestions_failed", tableErr.Error())
			return
		}

		response.Suggestions = suggestions
		s.writeJSON(w, http.StatusOK, response)
		return
	}

	webapi.WriteBadRequest(w, "connection_type_not_supported", fmt.Sprintf("connection '%s' does not support ingestr suggestions", connectionName))
}

func (s *webServer) handleGetSQLPathSuggestions(w http.ResponseWriter, r *http.Request) {
	assetID := strings.TrimSpace(chi.URLParam(r, "assetID"))
	if assetID == "" {
		webapi.WriteBadRequest(w, "asset_id_required", "asset ID is required")
		return
	}

	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	if prefix == "" {
		s.writeJSON(w, http.StatusOK, sqlPathSuggestionsResponse{
			Status:      "ok",
			Suggestions: []ingestrSuggestionItem{},
		})
		return
	}

	if _, _, _, err := s.resolveAssetByID(r.Context(), assetID); err != nil {
		webapi.WriteBadRequest(w, "asset_not_found", err.Error())
		return
	}

	response := sqlPathSuggestionsResponse{
		Status:      "ok",
		Suggestions: []ingestrSuggestionItem{},
	}

	switch {
	case strings.HasPrefix(prefix, "s3://"):
		environment := strings.TrimSpace(r.URL.Query().Get("environment"))
		items, err := s.buildSQLS3PathSuggestionItems(r.Context(), prefix, environment)
		if err != nil {
			webapi.WriteBadRequest(w, "sql_path_suggestions_failed", err.Error())
			return
		}
		response.Suggestions = items
	case strings.HasPrefix(prefix, "./"):
		items, err := buildWorkspacePathSuggestionItems(s.workspaceRoot, prefix)
		if err != nil {
			webapi.WriteBadRequest(w, "sql_path_suggestions_failed", err.Error())
			return
		}
		response.Suggestions = items
	case strings.HasPrefix(prefix, "/"):
		items, err := buildAbsolutePathSuggestionItems(prefix)
		if err != nil {
			webapi.WriteBadRequest(w, "sql_path_suggestions_failed", err.Error())
			return
		}
		response.Suggestions = items
	}

	s.writeJSON(w, http.StatusOK, response)
}

func sqlParseContextRangeResponseFromParser(input sqlintelligence.ParseContextRange) sqlParseContextRangeResponse {
	return sqlParseContextRangeResponse{
		Start:   input.Start,
		End:     input.End,
		Line:    input.Line,
		Col:     input.Col,
		EndLine: input.EndLine,
		EndCol:  input.EndCol,
	}
}

func sqlParseContextPartResponsesFromParser(input []sqlintelligence.ParseContextPart) []sqlParseContextPartResponse {
	result := make([]sqlParseContextPartResponse, 0, len(input))
	for _, part := range input {
		result = append(result, sqlParseContextPartResponse{
			Name:  part.Name,
			Kind:  part.Kind,
			Range: sqlParseContextRangeResponseFromParser(part.Range),
		})
	}

	return result
}

func sqlParseContextTableResponsesFromParser(input []sqlintelligence.ParseContextTable) []sqlParseContextTableResponse {
	result := make([]sqlParseContextTableResponse, 0, len(input))
	for _, table := range input {
		item := sqlParseContextTableResponse{
			Name:         table.Name,
			SourceKind:   table.SourceKind,
			ResolvedName: table.ResolvedName,
			Alias:        table.Alias,
			Parts:        sqlParseContextPartResponsesFromParser(table.Parts),
		}
		if table.AliasRange != nil {
			aliasRange := sqlParseContextRangeResponseFromParser(*table.AliasRange)
			item.AliasRange = &aliasRange
		}
		result = append(result, item)
	}

	return result
}

func sqlParseContextColumnResponsesFromParser(input []sqlintelligence.ParseContextColumn) []sqlParseContextColumnResponse {
	result := make([]sqlParseContextColumnResponse, 0, len(input))
	for _, column := range input {
		result = append(result, sqlParseContextColumnResponse{
			Name:          column.Name,
			Qualifier:     column.Qualifier,
			ResolvedTable: column.ResolvedTable,
			Parts:         sqlParseContextPartResponsesFromParser(column.Parts),
		})
	}

	return result
}

func buildSQLParseContextSchema(asset *pipeline.Asset, suggestionTables []struct {
	Name    string `json:"name"`
	Columns []struct {
		Name string `json:"name"`
		Type string `json:"type,omitempty"`
	} `json:"columns"`
}) sqlintelligence.Schema {
	schema := sqlintelligence.Schema{}

	for _, table := range suggestionTables {
		if strings.TrimSpace(table.Name) == "" {
			continue
		}

		columns := map[string]string{}
		for _, column := range table.Columns {
			if strings.TrimSpace(column.Name) == "" {
				continue
			}
			columns[column.Name] = strings.TrimSpace(column.Type)
		}

		if len(columns) > 0 {
			schema[table.Name] = columns
		}
	}

	if asset != nil && strings.TrimSpace(asset.Name) != "" && len(asset.Columns) > 0 {
		columns := map[string]string{}
		for _, column := range asset.Columns {
			if strings.TrimSpace(column.Name) == "" {
				continue
			}
			columns[column.Name] = strings.TrimSpace(column.Type)
		}
		if len(columns) > 0 {
			schema[asset.Name] = columns
		}
	}

	return schema
}

func sqlParseContextDiagnosticResponsesFromParser(input []sqlintelligence.ParseContextDiagnostic) []sqlParseContextDiagnosticResponse {
	result := make([]sqlParseContextDiagnosticResponse, 0, len(input))
	for _, diagnostic := range input {
		item := sqlParseContextDiagnosticResponse{
			Message:  diagnostic.Message,
			Severity: diagnostic.Severity,
		}
		if diagnostic.Range != nil {
			rangeValue := sqlParseContextRangeResponseFromParser(*diagnostic.Range)
			item.Range = &rangeValue
		}
		result = append(result, item)
	}

	return result
}

func (s *webServer) handleSQLParseContext(w http.ResponseWriter, r *http.Request) {
	var req sqlParseContextRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		webapi.WriteBadRequest(w, "invalid_request_body", err.Error())
		return
	}

	assetID := strings.TrimSpace(req.AssetID)
	if assetID == "" {
		webapi.WriteBadRequest(w, "asset_id_required", "asset_id is required")
		return
	}

	_, _, asset, err := s.resolveAssetByID(r.Context(), assetID)
	if err != nil {
		webapi.WriteBadRequest(w, "asset_not_found", err.Error())
		return
	}

	dialect, err := assetTypeToDialect(asset.Type)
	if err != nil {
		s.writeJSON(w, http.StatusOK, sqlParseContextResponse{
			Status:  "ok",
			AssetID: assetID,
			Errors:  []string{"unsupported SQL dialect for parse context"},
			Tables:  []sqlParseContextTableResponse{},
			Columns: []sqlParseContextColumnResponse{},
		})
		return
	}

	content := req.Content
	if strings.TrimSpace(content) == "" {
		content = asset.ExecutableFile.Content
	}
	schema := buildSQLParseContextSchema(asset, req.Schema)

	parseContext, err := sqlintelligence.ParseContextWithSchema(content, dialect, schema)
	if err != nil {
		s.writeJSON(w, http.StatusOK, sqlParseContextResponse{
			Status:  "error",
			AssetID: assetID,
			Dialect: dialect,
			Error:   err.Error(),
			Tables:  []sqlParseContextTableResponse{},
			Columns: []sqlParseContextColumnResponse{},
		})
		return
	}

	s.writeJSON(w, http.StatusOK, sqlParseContextResponse{
		Status:         "ok",
		AssetID:        assetID,
		Dialect:        dialect,
		QueryKind:      parseContext.QueryKind,
		IsSingleSelect: parseContext.IsSingleSelect,
		Tables:         sqlParseContextTableResponsesFromParser(parseContext.Tables),
		Columns:        sqlParseContextColumnResponsesFromParser(parseContext.Columns),
		Diagnostics:    sqlParseContextDiagnosticResponsesFromParser(parseContext.Diagnostics),
		Errors:         parseContext.Errors,
	})
}

type runRequest struct {
	Command    string   `json:"command"`
	PipelineID string   `json:"pipeline_id"`
	AssetPath  string   `json:"asset_path"`
	Args       []string `json:"args"`
}

func resolvePipelineRunTarget(pipelineID string) (string, error) {
	relPath, err := decodeID(pipelineID)
	if err != nil {
		return "", err
	}

	cleaned := filepath.Clean(relPath)
	base := strings.ToLower(filepath.Base(cleaned))
	if base == "pipeline.yml" || base == "pipeline.yaml" || base == ".pipeline.yml" || base == ".pipeline.yaml" {
		dir := filepath.Dir(cleaned)
		if dir == "." {
			return ".", nil
		}
		return filepath.ToSlash(dir), nil
	}

	return filepath.ToSlash(cleaned), nil
}

func (s *webServer) ResolvePipelineRunTarget(pipelineID string) error {
	_, err := resolvePipelineRunTarget(pipelineID)
	return err
}

func (s *webServer) newConnectionManager(ctx context.Context, environment string) (config.ConnectionAndDetailsGetter, error) {
	configPath := s.resolveConfigFilePath()
	cfg, err := config.LoadOrCreate(afero.NewOsFs(), configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	if environment != "" {
		if err := cfg.SelectEnvironment(environment); err != nil {
			return nil, fmt.Errorf("failed to select environment '%s': %w", environment, err)
		}
	}

	manager, errs := connection.NewManagerFromConfigWithContext(ctx, cfg)
	if len(errs) > 0 {
		return nil, errs[0]
	}

	return manager, nil
}

func databaseNameForConnectionDetails(details any) string {
	switch connectionDetails := details.(type) {
	case *config.PostgresConnection:
		return strings.TrimSpace(connectionDetails.Database)
	case *config.MySQLConnection:
		return strings.TrimSpace(connectionDetails.Database)
	case *config.MsSQLConnection:
		return strings.TrimSpace(connectionDetails.Database)
	case *config.ClickHouseConnection:
		return strings.TrimSpace(connectionDetails.Database)
	case *config.AthenaConnection:
		return strings.TrimSpace(connectionDetails.Database)
	case *config.SnowflakeConnection:
		return strings.TrimSpace(connectionDetails.Database)
	case *config.DatabricksConnection:
		return strings.TrimSpace(connectionDetails.Catalog)
	case *config.VerticaConnection:
		return strings.TrimSpace(connectionDetails.Database)
	default:
		return ""
	}
}

func buildSchemaTableSuggestionItems(tables map[string][]string, prefix string) []ingestrSuggestionItem {
	normalizedPrefix := strings.ToLower(strings.TrimSpace(prefix))
	items := make([]ingestrSuggestionItem, 0)

	schemas := make([]string, 0, len(tables))
	for schema := range tables {
		schemas = append(schemas, schema)
	}
	sort.Strings(schemas)

	for _, schema := range schemas {
		schemaTables := append([]string{}, tables[schema]...)
		sort.Strings(schemaTables)
		for _, table := range schemaTables {
			value := fmt.Sprintf("%s.%s", schema, table)
			if normalizedPrefix != "" && !strings.HasPrefix(strings.ToLower(value), normalizedPrefix) {
				continue
			}
			items = append(items, ingestrSuggestionItem{
				Value:  value,
				Kind:   "table",
				Detail: schema,
			})
		}
	}

	return limitSuggestionItems(items, 200)
}

func buildSQLDiscoveryTableItems(databaseName string, tables map[string][]string) []sqlDiscoveryTableItem {
	items := service.BuildSQLDiscoveryTableItems(databaseName, tables)
	result := make([]sqlDiscoveryTableItem, 0, len(items))
	for _, item := range items {
		result = append(result, sqlDiscoveryTableItem(item))
	}
	return result
}

func buildSQLDiscoveryTableItemsWithoutSchemas(databaseName string, tables []string) []sqlDiscoveryTableItem {
	items := service.BuildSQLDiscoveryTableItemsWithoutSchemas(databaseName, tables)
	result := make([]sqlDiscoveryTableItem, 0, len(items))
	for _, item := range items {
		result = append(result, sqlDiscoveryTableItem(item))
	}
	return result
}

func buildDuckDBSuggestionItems(
	ctx context.Context,
	fetcher interface {
		GetDatabases(ctx context.Context) ([]string, error)
		GetTables(ctx context.Context, databaseName string) ([]string, error)
	},
	prefix string,
) ([]ingestrSuggestionItem, error) {
	schemas, err := fetcher.GetDatabases(ctx)
	if err != nil {
		return nil, err
	}

	items := make([]ingestrSuggestionItem, 0)
	normalizedPrefix := strings.ToLower(strings.TrimSpace(prefix))
	sort.Strings(schemas)

	for _, schema := range schemas {
		tables, tableErr := fetcher.GetTables(ctx, schema)
		if tableErr != nil {
			return nil, tableErr
		}
		sort.Strings(tables)
		for _, table := range tables {
			fullName := fmt.Sprintf("%s.%s", schema, table)
			if normalizedPrefix != "" &&
				!strings.HasPrefix(strings.ToLower(fullName), normalizedPrefix) &&
				!strings.HasPrefix(strings.ToLower(table), normalizedPrefix) {
				continue
			}

			insertValue := fullName
			if strings.EqualFold(schema, "main") && normalizedPrefix != "" && !strings.Contains(prefix, ".") {
				insertValue = table
			}

			items = append(items, ingestrSuggestionItem{
				Value:  insertValue,
				Kind:   "table",
				Detail: schema,
			})
		}
	}

	return limitSuggestionItems(items, 200), nil
}

func buildS3SuggestionItems(
	ctx context.Context,
	conn interface {
		ListBuckets(ctx context.Context) ([]string, error)
		ListEntries(ctx context.Context, bucketName, prefix string) ([]string, error)
	},
	prefix string,
	connectionDetails any,
) ([]ingestrSuggestionItem, error) {
	normalizedPrefix := strings.TrimSpace(prefix)
	configuredBucket, configuredPrefix := s3SuggestionContext(connectionDetails)
	if configuredBucket != "" {
		lookupPrefix := normalizedPrefix
		if lookupPrefix == "" {
			lookupPrefix = configuredPrefix
		}

		items, err := conn.ListEntries(ctx, configuredBucket, lookupPrefix)
		if err != nil {
			return nil, err
		}

		return buildS3EntrySuggestionItems(items), nil
	}

	if normalizedPrefix == "" || !strings.Contains(normalizedPrefix, "/") {
		buckets, err := conn.ListBuckets(ctx)
		if err != nil {
			return nil, err
		}

		items := make([]ingestrSuggestionItem, 0, len(buckets))
		filter := strings.ToLower(normalizedPrefix)
		for _, bucket := range buckets {
			if filter != "" && !strings.HasPrefix(strings.ToLower(bucket), filter) {
				continue
			}
			items = append(items, ingestrSuggestionItem{
				Value:  bucket + "/",
				Kind:   "bucket",
				Detail: "S3 bucket",
			})
		}

		return limitSuggestionItems(items, 200), nil
	}

	bucketName, keyPrefix, _ := strings.Cut(normalizedPrefix, "/")
	items, err := conn.ListEntries(ctx, bucketName, keyPrefix)
	if err != nil {
		return nil, err
	}

	return buildS3EntrySuggestionItemsWithBucket(bucketName, items), nil
}

func (s *webServer) buildSQLS3PathSuggestionItems(
	ctx context.Context,
	prefix string,
	environment string,
) ([]ingestrSuggestionItem, error) {
	manager, err := s.newConnectionManager(ctx, environment)
	if err != nil {
		return nil, err
	}

	configPath := s.resolveConfigFilePath()
	cfg, err := config.LoadOrCreate(afero.NewOsFs(), configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	if environment != "" {
		if err := cfg.SelectEnvironment(environment); err != nil {
			return nil, fmt.Errorf("failed to select environment '%s': %w", environment, err)
		}
	}

	if cfg.SelectedEnvironment == nil || cfg.SelectedEnvironment.Connections == nil {
		return []ingestrSuggestionItem{}, nil
	}

	lookupPrefix := strings.TrimPrefix(prefix, "s3://")
	items := make([]ingestrSuggestionItem, 0)
	seen := make(map[string]struct{})
	var firstErr error

	for _, connConfig := range cfg.SelectedEnvironment.Connections.S3 {
		conn := manager.GetConnection(connConfig.Name)
		if conn == nil {
			continue
		}

		listableConn, ok := conn.(interface {
			ListBuckets(ctx context.Context) ([]string, error)
			ListEntries(ctx context.Context, bucketName, prefix string) ([]string, error)
		})
		if !ok {
			continue
		}

		connectionDetails := manager.GetConnectionDetails(connConfig.Name)
		if connectionDetails == nil {
			connectionDetails = &connConfig
		}

		connItems, itemErr := buildS3SuggestionItems(ctx, listableConn, lookupPrefix, connectionDetails)
		if itemErr != nil {
			if firstErr == nil {
				firstErr = itemErr
			}
			continue
		}

		for _, item := range connItems {
			item.Value = "s3://" + strings.TrimPrefix(item.Value, "s3://")
			if item.Detail != "" {
				item.Detail = fmt.Sprintf("%s (%s)", item.Detail, connConfig.Name)
			} else {
				item.Detail = connConfig.Name
			}

			key := strings.ToLower(item.Value) + "::" + item.Kind
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			items = append(items, item)
		}
	}

	if len(items) == 0 && firstErr != nil {
		return nil, firstErr
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Value < items[j].Value
	})

	return limitSuggestionItems(items, 200), nil
}

func s3SuggestionContext(connectionDetails any) (string, string) {
	s3Connection, ok := connectionDetails.(*config.S3Connection)
	if !ok || s3Connection == nil {
		return "", ""
	}

	bucketName := strings.TrimSpace(s3Connection.BucketName)
	pathPrefix := strings.TrimSpace(s3Connection.PathToFile)
	if pathPrefix != "" && !strings.HasSuffix(pathPrefix, "/") {
		pathPrefix += "/"
	}

	return bucketName, pathPrefix
}

func buildS3EntrySuggestionItems(items []string) []ingestrSuggestionItem {
	suggestions := make([]ingestrSuggestionItem, 0, len(items))
	for _, item := range items {
		kind := "file"
		detail := "S3 object"
		if strings.HasSuffix(item, "/") {
			kind = "prefix"
			detail = "S3 prefix"
		}
		suggestions = append(suggestions, ingestrSuggestionItem{
			Value:  item,
			Kind:   kind,
			Detail: detail,
		})
	}
	return limitSuggestionItems(suggestions, 200)
}

// DuckDB queries executed from Bruin Web inherit the workspace root as cwd,
// so relative file suggestions should resolve from that same location.
func buildWorkspacePathSuggestionItems(workspaceRoot string, prefix string) ([]ingestrSuggestionItem, error) {
	relativePrefix := strings.TrimPrefix(prefix, "./")
	searchDir, typedDirectory, fragment := splitRelativePathLookup(workspaceRoot, relativePrefix, prefix)

	entries, err := os.ReadDir(searchDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []ingestrSuggestionItem{}, nil
		}
		return nil, err
	}

	items := make([]ingestrSuggestionItem, 0, len(entries))
	filter := strings.ToLower(fragment)
	for _, entry := range entries {
		name := entry.Name()
		if filter != "" && !strings.HasPrefix(strings.ToLower(name), filter) {
			continue
		}

		displayPath := "./" + name
		if typedDirectory != "" {
			displayPath = "./" + filepath.ToSlash(filepath.Join(typedDirectory, name))
		}

		kind := "file"
		detail := "Workspace file"
		if entry.IsDir() {
			displayPath += "/"
			kind = "directory"
			detail = "Workspace directory"
		}

		items = append(items, ingestrSuggestionItem{
			Value:  displayPath,
			Kind:   kind,
			Detail: detail,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Value < items[j].Value
	})

	return limitSuggestionItems(items, 200), nil
}

func buildAbsolutePathSuggestionItems(prefix string) ([]ingestrSuggestionItem, error) {
	searchDir, displayDirectory, fragment := splitAbsolutePathLookup(prefix)

	entries, err := os.ReadDir(searchDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []ingestrSuggestionItem{}, nil
		}
		return nil, err
	}

	items := make([]ingestrSuggestionItem, 0, len(entries))
	filter := strings.ToLower(fragment)
	for _, entry := range entries {
		name := entry.Name()
		if filter != "" && !strings.HasPrefix(strings.ToLower(name), filter) {
			continue
		}

		displayPath := filepath.ToSlash(filepath.Join(displayDirectory, name))
		if displayDirectory == string(filepath.Separator) {
			displayPath = string(filepath.Separator) + name
		}

		kind := "file"
		detail := "Local file"
		if entry.IsDir() {
			displayPath += "/"
			kind = "directory"
			detail = "Local directory"
		}

		items = append(items, ingestrSuggestionItem{
			Value:  displayPath,
			Kind:   kind,
			Detail: detail,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Value < items[j].Value
	})

	return limitSuggestionItems(items, 200), nil
}

func splitRelativePathLookup(workspaceRoot string, relativePrefix string, rawPrefix string) (string, string, string) {
	trimmed := relativePrefix
	if strings.HasSuffix(rawPrefix, "/") {
		typedDirectory := strings.TrimSuffix(trimmed, "/")
		if typedDirectory == "." {
			typedDirectory = ""
		}
		return filepath.Join(workspaceRoot, typedDirectory), typedDirectory, ""
	}

	fragment := filepath.Base(trimmed)
	typedDirectory := filepath.Dir(trimmed)
	if typedDirectory == "." {
		typedDirectory = ""
	}

	return filepath.Join(workspaceRoot, typedDirectory), typedDirectory, fragment
}

func splitAbsolutePathLookup(prefix string) (string, string, string) {
	if strings.HasSuffix(prefix, "/") {
		directory := filepath.Clean(prefix)
		if directory == "." {
			directory = string(filepath.Separator)
		}
		return directory, directory, ""
	}

	searchDir := filepath.Dir(prefix)
	displayDirectory := searchDir
	if displayDirectory == "." {
		displayDirectory = string(filepath.Separator)
	}

	return searchDir, displayDirectory, filepath.Base(prefix)
}

func buildS3EntrySuggestionItemsWithBucket(bucketName string, items []string) []ingestrSuggestionItem {
	suggestions := make([]ingestrSuggestionItem, 0, len(items))
	for _, item := range items {
		kind := "file"
		detail := "S3 object"
		if strings.HasSuffix(item, "/") {
			kind = "prefix"
			detail = "S3 prefix"
		}
		suggestions = append(suggestions, ingestrSuggestionItem{
			Value:  bucketName + "/" + item,
			Kind:   kind,
			Detail: detail,
		})
	}

	return limitSuggestionItems(suggestions, 200)
}

func limitSuggestionItems(items []ingestrSuggestionItem, max int) []ingestrSuggestionItem {
	if max <= 0 || len(items) <= max {
		return items
	}
	return items[:max]
}

func (s *webServer) MaterializePipelineStream(ctx context.Context, pipelineID string, onChunk func([]byte)) executionMaterializeEvent {
	return executionMaterializeEvent(s.executionSvc.MaterializePipelineStream(ctx, pipelineID, onChunk))
}

func (s *webServer) handleRun(w http.ResponseWriter, r *http.Request) {
	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		webapi.WriteBadRequest(w, "invalid_request_body", err.Error())
		return
	}

	command := req.Command
	if command == "" {
		command = "run"
	}

	if !service.IsCommandAllowed(command) {
		webapi.WriteBadRequest(w, "command_not_allowed",
			fmt.Sprintf("command %q is not allowed; permitted commands: run, query, patch, lint", command))
		return
	}

	target := "."
	if req.PipelineID != "" {
		relPath, err := resolvePipelineRunTarget(req.PipelineID)
		if err != nil {
			webapi.WriteBadRequest(w, "invalid_pipeline_id", "invalid pipeline id")
			return
		}
		target = relPath
	}

	if req.AssetPath != "" {
		target = req.AssetPath
	}

	cmdArgs := append([]string{command, target}, req.Args...)
	output, err := s.runner.Run(r.Context(), cmdArgs)
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]any{
			"status":    "error",
			"command":   cmdArgs,
			"output":    string(output),
			"error":     err.Error(),
			"exit_code": 1,
		})
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"command":   cmdArgs,
		"output":    string(output),
		"exit_code": 0,
	})
}

func (s *webServer) handleStatic(w http.ResponseWriter, r *http.Request) {
	if s.staticHandler != nil {
		s.staticHandler.ServeHTTP(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("Bruin Web UI assets are unavailable."))
}

// suppressWatcherFor marks a path as recently handled by a server-initiated
// write (API handler or patch timer). Filesystem watcher events for this
// path will be suppressed for a short window to avoid duplicate notifications.
func (s *webServer) suppressWatcherFor(eventPath string) {
	normalized := filepath.ToSlash(eventPath)
	s.recentServerWritesMu.Lock()
	s.recentServerWrites[normalized] = time.Now()
	s.recentServerWritesMu.Unlock()
}

// isWatcherSuppressed returns true if the given path was recently handled by
// a server-initiated write and the filesystem watcher event should be skipped.
func (s *webServer) isWatcherSuppressed(eventPath string) bool {
	normalized := filepath.ToSlash(eventPath)
	s.recentServerWritesMu.Lock()
	defer s.recentServerWritesMu.Unlock()
	t, ok := s.recentServerWrites[normalized]
	if !ok {
		return false
	}
	if time.Since(t) < 2*time.Second {
		return true
	}
	delete(s.recentServerWrites, normalized)
	return false
}

func (s *webServer) pushWorkspaceUpdate(ctx context.Context, eventType, eventPath string) {
	_ = s.refreshWorkspace(ctx)
	state := s.currentState()
	// For file changes: only the directly edited asset needs re-inspection.
	// Its SQL changed, but no table data changed yet — downstreams still query
	// the same underlying tables and get identical results.
	changed := s.findDirectlyChangedAssetIDs(filepath.ToSlash(eventPath))

	// Record content-change timestamps in the freshness tracker.
	now := time.Now().UTC()
	for _, id := range changed {
		if name := s.findAssetNameByID(id); name != "" {
			s.freshness.RecordContentChange(name, now)
		}
	}

	s.hub.Publish(workspaceEvent{
		Type:            eventType,
		Path:            filepath.ToSlash(eventPath),
		Workspace:       stripAssetContent(state),
		Lite:            true,
		ChangedAssetIDs: changed,
	})
}

// pushWorkspaceUpdateImmediate publishes immediately (bypasses debounce).
// Used by API handlers that need the client to see the change right away.
func (s *webServer) pushWorkspaceUpdateImmediate(ctx context.Context, eventType, eventPath string) {
	s.pushWorkspaceUpdateImmediateWithChangedIDs(ctx, eventType, eventPath, nil)
}

func (s *webServer) pushWorkspaceUpdateImmediateWithChangedIDs(ctx context.Context, eventType, eventPath string, changedAssetIDs []string) {
	_ = s.refreshWorkspace(ctx)
	state := s.currentState()
	changed := changedAssetIDs
	if len(changed) == 0 {
		changed = s.findDirectlyChangedAssetIDs(filepath.ToSlash(eventPath))
	}

	now := time.Now().UTC()
	for _, id := range changed {
		if name := s.findAssetNameByID(id); name != "" {
			s.freshness.RecordContentChange(name, now)
		}
	}

	s.hub.PublishImmediate(workspaceEvent{
		Type:            eventType,
		Path:            filepath.ToSlash(eventPath),
		Workspace:       stripAssetContentKeepingIDs(state, changed),
		Lite:            true,
		ChangedAssetIDs: changed,
	})
}

// stripAssetContent returns a copy of the workspace state with asset Content
// fields emptied. This dramatically reduces SSE payload size.
// The full state (with content) is still available via GET /api/workspace.
func stripAssetContent(state workspaceState) workspaceState {
	lite := state
	lite.Pipelines = make([]webPipeline, len(state.Pipelines))
	for i, p := range state.Pipelines {
		litePipeline := p
		litePipeline.Assets = make([]webAsset, len(p.Assets))
		for j, a := range p.Assets {
			a.Content = ""
			litePipeline.Assets[j] = a
		}
		lite.Pipelines[i] = litePipeline
	}
	return lite
}

func stripAssetContentKeepingIDs(state workspaceState, keepIDs []string) workspaceState {
	if len(keepIDs) == 0 {
		return stripAssetContent(state)
	}

	keep := make(map[string]struct{}, len(keepIDs))
	for _, id := range keepIDs {
		keep[id] = struct{}{}
	}

	lite := state
	lite.Pipelines = make([]webPipeline, len(state.Pipelines))
	for i, p := range state.Pipelines {
		litePipeline := p
		litePipeline.Assets = make([]webAsset, len(p.Assets))
		for j, a := range p.Assets {
			if _, ok := keep[a.ID]; !ok {
				a.Content = ""
			}
			litePipeline.Assets[j] = a
		}
		lite.Pipelines[i] = litePipeline
	}

	return lite
}

// assetEntry is a lightweight struct used by the asset-graph helpers.
type assetEntry struct {
	id        string
	name      string
	path      string
	upstreams []string
}

// buildAssetIndex creates the full asset list and name→ID mapping from the
// current workspace state. Callers should not hold stateMu.
func (s *webServer) buildAssetIndex() ([]assetEntry, map[string]string) {
	state := s.currentState()
	var all []assetEntry
	nameToID := make(map[string]string)
	for _, p := range state.Pipelines {
		for _, a := range p.Assets {
			all = append(all, assetEntry{
				id:        a.ID,
				name:      a.Name,
				path:      a.Path,
				upstreams: a.Upstreams,
			})
			nameToID[a.Name] = a.ID
		}
	}
	return all, nameToID
}

// buildDownstreamIndex returns assetID → list of direct downstream IDs.
func buildDownstreamIndex(assets []assetEntry, nameToID map[string]string) map[string][]string {
	downstream := make(map[string][]string)
	for _, a := range assets {
		for _, upName := range a.upstreams {
			if upID, ok := nameToID[upName]; ok {
				downstream[upID] = append(downstream[upID], a.id)
			}
		}
	}
	return downstream
}

// findDirectlyChangedAssetIDs returns only the asset IDs whose source file
// matches the given event path. No downstream expansion — used for file-edit
// events where only the edited asset's inspect result would change (its SQL
// changed, but no table data changed yet).
func (s *webServer) findDirectlyChangedAssetIDs(eventPath string) []string {
	assets, _ := s.buildAssetIndex()
	normalizedEvent := filepath.ToSlash(eventPath)

	var result []string
	for _, a := range assets {
		if pathContains(normalizedEvent, a.path) {
			result = append(result, a.id)
		}
	}
	sort.Strings(result)
	return result
}

// findMaterializationInspectIDs returns the given asset IDs plus their direct
// (1-level) downstream dependents. Used after materialization — the materialized
// asset's table now has new data, so queries that read from it (direct
// downstreams) may return different results. Transitive downstreams (2+ hops)
// still read from the direct downstream's un-materialized table, so they are
// not affected for inspect purposes.
func (s *webServer) findMaterializationInspectIDs(assetIDs ...string) []string {
	assets, nameToID := s.buildAssetIndex()
	downstream := buildDownstreamIndex(assets, nameToID)

	seen := make(map[string]struct{})
	for _, id := range assetIDs {
		seen[id] = struct{}{}
		for _, child := range downstream[id] {
			seen[child] = struct{}{}
		}
	}

	result := make([]string, 0, len(seen))
	for id := range seen {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

// findAllDownstreamIDs returns the given asset IDs plus ALL transitive
// downstream dependents (BFS). Used for materialization-staleness tracking:
// if asset A's content changes, A and every transitive dependent is stale
// from a materialization perspective.
func (s *webServer) findAllDownstreamIDs(eventPath string) []string {
	assets, nameToID := s.buildAssetIndex()
	normalizedEvent := filepath.ToSlash(eventPath)
	downstream := buildDownstreamIndex(assets, nameToID)

	// Find directly changed assets.
	roots := make(map[string]struct{})
	for _, a := range assets {
		if pathContains(normalizedEvent, a.path) {
			roots[a.id] = struct{}{}
		}
	}
	if len(roots) == 0 {
		return nil
	}

	// BFS to expand all transitive downstreams.
	visited := make(map[string]struct{})
	queue := make([]string, 0, len(roots))
	for id := range roots {
		queue = append(queue, id)
		visited[id] = struct{}{}
	}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, child := range downstream[current] {
			if _, ok := visited[child]; !ok {
				visited[child] = struct{}{}
				queue = append(queue, child)
			}
		}
	}

	result := make([]string, 0, len(visited))
	for id := range visited {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

// pathContains checks whether eventPath matches or is a parent of assetPath.
func pathContains(eventPath, assetPath string) bool {
	eventPath = filepath.ToSlash(filepath.Clean(eventPath))
	assetPath = filepath.ToSlash(filepath.Clean(assetPath))

	if eventPath == assetPath {
		return true
	}
	// The event might be a directory change that contains the asset file.
	if strings.HasPrefix(assetPath, eventPath+"/") {
		return true
	}

	base := filepath.Base(eventPath)

	// If the pipeline manifest changes, all assets under that pipeline's assets/
	// folder should be considered affected.
	if base == "pipeline.yml" || base == ".pipeline.yml" {
		assetsDir := filepath.ToSlash(filepath.Join(filepath.Dir(eventPath), "assets"))
		if strings.HasPrefix(assetPath, assetsDir+"/") {
			return true
		}
	}

	// Asset-level metadata files affect assets in the same folder.
	if base == "asset.yml" || base == ".asset.yml" ||
		base == "schema.yml" || base == "schema.yaml" ||
		base == "checks.yml" || base == "checks.yaml" ||
		base == "source.yml" || base == "source.yaml" {
		if filepath.Dir(eventPath) == filepath.Dir(assetPath) {
			return true
		}
	}

	return false
}

// findAssetNameByID looks up the asset name for a given encoded asset ID
// from the current workspace state.
func (s *webServer) findAssetNameByID(assetID string) string {
	state := s.currentState()
	for _, p := range state.Pipelines {
		for _, a := range p.Assets {
			if a.ID == assetID {
				return a.Name
			}
		}
	}
	return ""
}

// handleGetAssetFreshness returns freshness timestamps for all tracked assets.
// Each entry includes both materialization and content-change timestamps so
// the frontend can compute staleness from either perspective.
func (s *webServer) handleGetAssetFreshness(w http.ResponseWriter, _ *http.Request) {
	all := s.freshness.GetAll()

	type assetFreshnessEntry struct {
		AssetName          string     `json:"asset_name"`
		MaterializedAt     *time.Time `json:"materialized_at,omitempty"`
		MaterializedStatus string     `json:"materialized_status,omitempty"`
		ContentChangedAt   *time.Time `json:"content_changed_at,omitempty"`
	}

	entries := make([]assetFreshnessEntry, 0, len(all))
	for name, ts := range all {
		entry := assetFreshnessEntry{
			AssetName:          name,
			MaterializedAt:     ts.MaterializedAt,
			MaterializedStatus: ts.MaterializedStatus,
			ContentChangedAt:   ts.ContentChangedAt,
		}
		entries = append(entries, entry)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].AssetName < entries[j].AssetName
	})

	s.writeJSON(w, http.StatusOK, map[string]any{
		"assets": entries,
	})
}

func safeJoin(root, relPath string) (string, error) {
	return service.SafeJoin(root, relPath)
}

func pipelinePathsReferToSameRoot(left, right string) bool {
	if strings.TrimSpace(left) == "" || strings.TrimSpace(right) == "" {
		return false
	}

	normalize := func(path string) string {
		cleaned := filepath.Clean(path)
		base := strings.ToLower(filepath.Base(cleaned))
		if base == "pipeline.yml" || base == "pipeline.yaml" || base == ".pipeline.yml" || base == ".pipeline.yaml" {
			return filepath.Dir(cleaned)
		}
		return cleaned
	}

	return normalize(left) == normalize(right)
}

func slug(input string) string {
	return strings.ReplaceAll(service.Slug(strings.ReplaceAll(input, ".", " ")), "-", "_")
}

func extensionForAssetType(assetType string) string {
	assetType = strings.ToLower(assetType)
	if strings.Contains(assetType, "python") || strings.HasSuffix(assetType, ".py") {
		return ".py"
	}
	if strings.Contains(assetType, "ingestr") {
		return ".asset.yaml"
	}
	if strings.Contains(assetType, "r") || strings.HasSuffix(assetType, ".r") {
		return ".r"
	}
	return ".sql"
}

func inferAssetTypeFromPath(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".py":
		return "python"
	case ".r":
		return "r"
	case ".yml", ".yaml":
		return "yaml"
	default:
		return "duckdb.sql"
	}
}

func deriveDownstreamAssetName(sourceAssetName string, parsedPipeline *pipeline.Pipeline) string {
	trimmed := strings.TrimSpace(sourceAssetName)
	if trimmed == "" {
		trimmed = "asset"
	}

	prefix := ""
	leaf := trimmed
	if lastDot := strings.LastIndex(trimmed, "."); lastDot >= 0 {
		prefix = trimmed[:lastDot]
		leaf = trimmed[lastDot+1:]
	}

	baseLeaf := leaf + "_child"
	existing := make(map[string]struct{})
	if parsedPipeline != nil {
		for _, asset := range parsedPipeline.Assets {
			existing[strings.ToLower(strings.TrimSpace(asset.Name))] = struct{}{}
		}
	}

	for index := 1; ; index++ {
		candidateLeaf := fmt.Sprintf("%s_%d", baseLeaf, index)
		candidate := candidateLeaf
		if prefix != "" {
			candidate = prefix + "." + candidateLeaf
		}

		if _, ok := existing[strings.ToLower(candidate)]; !ok {
			return candidate
		}
	}
}

func deriveSQLAssetTypeForSource(sourceAsset *pipeline.Asset, parsedPipeline *pipeline.Pipeline, sourceConnectionName string) string {
	if sourceAsset == nil {
		return string(pipeline.AssetTypeDuckDBQuery)
	}

	lowerType := strings.ToLower(string(sourceAsset.Type))
	if strings.Contains(lowerType, "sql") {
		return string(sourceAsset.Type)
	}

	if lowerType == "ingestr" {
		if destination := strings.TrimSpace(strings.ToLower(sourceAsset.Parameters["destination"])); destination != "" {
			if mapped, ok := pipeline.IngestrTypeConnectionMapping[destination]; ok {
				return string(mapped)
			}
		}
	}

	if connectionType, ok := pipeline.AssetTypeConnectionMapping[sourceAsset.Type]; ok {
		if assetType := preferredSQLAssetTypeForConnectionType(connectionType); assetType != "" {
			return assetType
		}
	}

	if parsedPipeline != nil && sourceConnectionName != "" {
		if connectionType := resolveConnectionTypeForName(parsedPipeline, sourceConnectionName); connectionType != "" {
			if assetType := preferredSQLAssetTypeForConnectionType(connectionType); assetType != "" {
				return assetType
			}
		}
	}

	if parsedPipeline != nil {
		return string(parsedPipeline.GetMajorityAssetTypesFromSQLAssets(pipeline.AssetTypeDuckDBQuery))
	}

	return string(pipeline.AssetTypeDuckDBQuery)
}

func preferredSQLAssetTypeForConnectionType(connectionType string) string {
	switch strings.ToLower(strings.TrimSpace(connectionType)) {
	case "google_cloud_platform":
		return string(pipeline.AssetTypeBigqueryQuery)
	case "snowflake":
		return string(pipeline.AssetTypeSnowflakeQuery)
	case "postgres":
		return string(pipeline.AssetTypePostgresQuery)
	case "redshift":
		return string(pipeline.AssetTypeRedshiftQuery)
	case "mssql":
		return string(pipeline.AssetTypeMsSQLQuery)
	case "fabric":
		return string(pipeline.AssetTypeFabricQuery)
	case "databricks":
		return string(pipeline.AssetTypeDatabricksQuery)
	case "synapse":
		return string(pipeline.AssetTypeSynapseQuery)
	case "athena":
		return string(pipeline.AssetTypeAthenaQuery)
	case "duckdb":
		return string(pipeline.AssetTypeDuckDBQuery)
	case "motherduck":
		return string(pipeline.AssetTypeMotherduckQuery)
	case "clickhouse":
		return string(pipeline.AssetTypeClickHouse)
	case "trino":
		return string(pipeline.AssetTypeTrinoQuery)
	case "oracle":
		return string(pipeline.AssetTypeOracleQuery)
	case "mysql":
		return string(pipeline.AssetTypeMySQLQuery)
	case "vertica":
		return string(pipeline.AssetTypeVerticaQuery)
	default:
		return ""
	}
}

func resolveConnectionTypeForName(parsedPipeline *pipeline.Pipeline, connectionName string) string {
	if parsedPipeline == nil || strings.TrimSpace(connectionName) == "" {
		return ""
	}

	normalizedConnectionName := strings.TrimSpace(connectionName)

	for connectionType, configuredName := range parsedPipeline.DefaultConnections {
		if strings.EqualFold(strings.TrimSpace(configuredName), normalizedConnectionName) {
			return connectionType
		}
	}

	for _, connectionType := range supportedSQLConnectionTypes() {
		if strings.EqualFold(defaultConnectionNameForType(connectionType), normalizedConnectionName) {
			return connectionType
		}
	}

	return ""
}

func supportedSQLConnectionTypes() []string {
	return []string{
		"google_cloud_platform",
		"snowflake",
		"postgres",
		"redshift",
		"mssql",
		"fabric",
		"databricks",
		"synapse",
		"athena",
		"duckdb",
		"motherduck",
		"clickhouse",
		"trino",
		"oracle",
		"mysql",
		"vertica",
	}
}

func defaultConnectionNameForType(connectionType string) string {
	switch strings.ToLower(strings.TrimSpace(connectionType)) {
	case "google_cloud_platform":
		return "gcp-default"
	default:
		return strings.ToLower(strings.TrimSpace(connectionType)) + "-default"
	}
}

func defaultAssetContent(assetName, assetType, assetPath string) string {
	base := service.DefaultAssetContent(assetName, assetType, assetPath)
	if strings.HasSuffix(strings.ToLower(assetPath), ".sql") {
		return fmt.Sprintf("/* @bruin\n\nname: %s\ntype: %s\nmaterialization:\n  type: view\n\n@bruin */\n", assetName, assetType)
	}
	return base
}

func defaultDerivedSQLAssetContent(assetName, assetType, assetPath, sourceAssetName, connectionName string) string {
	return service.DefaultDerivedSQLAssetContent(assetName, assetType, assetPath, sourceAssetName, connectionName)
}

func ensurePythonRequirementsFile(absAssetPath, assetType, relAssetPath string) error {
	return service.EnsurePythonRequirementsFile(absAssetPath, assetType, relAssetPath)
}

func splitBruinHeader(content string) (header string, separator string, body string, found bool) {
	return service.SplitBruinHeader(content)
}

func extractExecutableContent(content string) string {
	return service.ExtractExecutableContent(content)
}

func mergeExecutableContent(currentFileContent, executableContent string) string {
	return service.MergeExecutableContent(currentFileContent, executableContent)
}

func encodeID(value string) string {
	return service.EncodeID(value)
}

func decodeID(value string) (string, error) {
	return service.DecodeID(value)
}

func (s *webServer) runConnectionQueryForEnvironment(ctx context.Context, connectionName, environment, query string) ([]string, []map[string]any, error) {
	return s.executionSvc.RunConnectionQueryForEnvironment(ctx, connectionName, environment, query)
}

func (s *webServer) ColumnValues(ctx context.Context, connectionName, environment, query string) service.SQLColumnValuesResult {
	return s.sqlSvc.ColumnValues(ctx, connectionName, environment, query)
}

func (s *webServer) Databases(ctx context.Context, connectionName, environment string) (service.SQLDatabaseDiscoveryResult, *service.SQLAPIError) {
	return s.sqlSvc.Databases(ctx, connectionName, environment)
}

func (s *webServer) Tables(ctx context.Context, connectionName, databaseName, environment string) (service.SQLTableDiscoveryResult, *service.SQLAPIError) {
	return s.sqlSvc.Tables(ctx, connectionName, databaseName, environment)
}

func (s *webServer) TableColumns(ctx context.Context, connectionName, tableName, environment string) (service.SQLTableColumnsResult, int) {
	return s.sqlSvc.TableColumns(ctx, connectionName, tableName, environment)
}

func extractColumnNames(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return []string{}
	}

	columns := make([]string, 0, len(items))
	for _, item := range items {
		if name, ok := item.(string); ok {
			columns = append(columns, name)
			continue
		}

		columnMap, ok := item.(map[string]any)
		if !ok {
			continue
		}

		nameValue, ok := columnMap["name"]
		if !ok {
			continue
		}

		if name, ok := nameValue.(string); ok {
			columns = append(columns, name)
		}
	}

	return columns
}

func castRows(value any) []map[string]any {
	items, ok := value.([]any)
	if !ok {
		return []map[string]any{}
	}

	rows := make([]map[string]any, 0, len(items))
	for _, item := range items {
		row, ok := item.(map[string]any)
		if ok {
			rows = append(rows, row)
		}
	}

	return rows
}

func castRowsByColumns(value any, columns []string) []map[string]any {
	if len(columns) == 0 {
		return []map[string]any{}
	}

	items, ok := value.([]any)
	if !ok {
		return []map[string]any{}
	}

	rows := make([]map[string]any, 0, len(items))
	for _, item := range items {
		cellValues, ok := item.([]any)
		if !ok {
			continue
		}

		row := make(map[string]any, len(columns))
		for idx, column := range columns {
			if idx < len(cellValues) {
				row[column] = cellValues[idx]
				continue
			}
			row[column] = nil
		}

		rows = append(rows, row)
	}

	return rows
}

func inferColumns(rows []map[string]any) []string {
	if len(rows) == 0 {
		return []string{}
	}

	columns := make([]string, 0)
	for key := range rows[0] {
		columns = append(columns, key)
	}
	sort.Strings(columns)
	return columns
}

func inferWebColumnsFromQueryOutput(output []byte) []webColumn {
	columns := service.InferSQLColumnsFromQueryOutput(output)
	result := make([]webColumn, 0, len(columns))
	for _, column := range columns {
		result = append(result, webColumn{Name: column.Name, Type: column.Type})
	}
	return result
}

func pipelineColumnsToWebColumns(columns []pipeline.Column) []webColumn {
	result := make([]webColumn, 0, len(columns))
	for _, column := range columns {
		var nullable *bool
		if column.Nullable.Value != nil {
			value := *column.Nullable.Value
			nullable = &value
		}

		checks := make([]webColumnCheck, 0, len(column.Checks))
		for _, check := range column.Checks {
			checks = append(checks, webColumnCheck{
				Name:        check.Name,
				Value:       columnCheckValueToAny(check.Value),
				Blocking:    check.Blocking.Value,
				Description: check.Description,
			})
		}

		result = append(result, webColumn{
			Name:          column.Name,
			Type:          column.Type,
			Description:   column.Description,
			Tags:          column.Tags,
			PrimaryKey:    column.PrimaryKey,
			UpdateOnMerge: column.UpdateOnMerge,
			MergeSQL:      column.MergeSQL,
			Nullable:      nullable,
			Owner:         column.Owner,
			Domains:       column.Domains,
			Meta:          column.Meta,
			Checks:        checks,
		})
	}

	return result
}

func webColumnsToPipelineColumns(columns []webColumn) []pipeline.Column {
	result := make([]pipeline.Column, 0, len(columns))
	for _, column := range columns {
		checks := make([]pipeline.ColumnCheck, 0, len(column.Checks))
		for _, check := range column.Checks {
			checks = append(checks, pipeline.ColumnCheck{
				Name:        check.Name,
				Value:       anyToColumnCheckValue(check.Value),
				Blocking:    pipeline.DefaultTrueBool{Value: check.Blocking},
				Description: check.Description,
			})
		}

		result = append(result, pipeline.Column{
			Name:          column.Name,
			Type:          column.Type,
			Description:   column.Description,
			Tags:          column.Tags,
			PrimaryKey:    column.PrimaryKey,
			UpdateOnMerge: column.UpdateOnMerge,
			MergeSQL:      column.MergeSQL,
			Nullable:      pipeline.DefaultTrueBool{Value: column.Nullable},
			Owner:         column.Owner,
			Domains:       column.Domains,
			Meta:          column.Meta,
			Checks:        checks,
		})
	}

	return result
}

func columnCheckValueToAny(value pipeline.ColumnCheckValue) any {
	if value.IntArray != nil {
		return *value.IntArray
	}
	if value.Int != nil {
		return *value.Int
	}
	if value.Float != nil {
		return *value.Float
	}
	if value.StringArray != nil {
		return *value.StringArray
	}
	if value.String != nil {
		return *value.String
	}
	if value.Bool != nil {
		return *value.Bool
	}

	return nil
}

func anyToColumnCheckValue(value any) pipeline.ColumnCheckValue {
	result := pipeline.ColumnCheckValue{}
	if value == nil {
		return result
	}

	switch v := value.(type) {
	case bool:
		result.Bool = &v
	case string:
		result.String = &v
	case int:
		result.Int = &v
	case int64:
		converted := int(v)
		result.Int = &converted
	case float64:
		if v == float64(int(v)) {
			converted := int(v)
			result.Int = &converted
		} else {
			result.Float = &v
		}
	case []string:
		result.StringArray = &v
	case []any:
		stringArr := make([]string, 0, len(v))
		intArr := make([]int, 0, len(v))
		allStrings := true
		allInts := true
		for _, item := range v {
			s, sOK := item.(string)
			if sOK {
				stringArr = append(stringArr, s)
			} else {
				allStrings = false
			}

			n, nOK := item.(float64)
			if nOK && n == float64(int(n)) {
				intArr = append(intArr, int(n))
			} else {
				allInts = false
			}
		}

		if allStrings {
			result.StringArray = &stringArr
		} else if allInts {
			result.IntArray = &intArr
		}
	}

	return result
}

func buildWorkspaceConfigConnectionTypes() []service.WorkspaceConfigConnectionType {
	return service.BuildWorkspaceConfigConnectionTypes()
}
