package cmd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	webapi "github.com/bruin-data/bruin/internal/web/api"
	"github.com/bruin-data/bruin/internal/web/events"
	"github.com/bruin-data/bruin/internal/web/freshness"
	"github.com/bruin-data/bruin/internal/web/service"
	"github.com/bruin-data/bruin/internal/web/watch"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	bruinpath "github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
	"gopkg.in/yaml.v3"
)

type webAsset struct {
	ID                  string            `json:"id"`
	Name                string            `json:"name"`
	Type                string            `json:"type"`
	Path                string            `json:"path"`
	Content             string            `json:"content"`
	Upstreams           []string          `json:"upstreams"`
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

type assetMaterializationState struct {
	AssetID         string `json:"asset_id"`
	IsMaterialized  bool   `json:"is_materialized"`
	MaterializedAs  string `json:"materialized_as,omitempty"`
	FreshnessStatus string `json:"freshness_status,omitempty"`
	RowCount        *int64 `json:"row_count,omitempty"`
	Connection      string `json:"connection,omitempty"`
	DeclaredMatType string `json:"materialization_type,omitempty"`
}

type pipelineMaterializationResponse struct {
	PipelineID string                      `json:"pipeline_id"`
	Assets     []assetMaterializationState `json:"assets"`
}

type pipelineMaterializationInfo struct {
	AssetName       string
	Connection      string
	IsMaterialized  bool
	MaterializedAs  string
	FreshnessStatus string
	RowCount        *int64
	DeclaredMatType string
}

type ingestrSuggestionItem struct {
	Value  string `json:"value"`
	Kind   string `json:"kind,omitempty"`
	Detail string `json:"detail,omitempty"`
}

type ingestrSuggestionsResponse struct {
	Status         string                  `json:"status"`
	ConnectionType string                  `json:"connection_type,omitempty"`
	Suggestions    []ingestrSuggestionItem `json:"suggestions"`
	Error          string                  `json:"error,omitempty"`
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

type webServer struct {
	workspaceRoot string
	staticDir     string
	watchMode     string
	watchPoll     time.Duration

	stateMu  sync.RWMutex
	state    workspaceState
	revision atomic.Int64

	patchMu     sync.Mutex
	patchTimers map[string]*time.Timer

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

var (
	sqlBruinHeaderPattern    = regexp.MustCompile(`(?s)\A(\s*/\*\s*@bruin.*?@bruin\s*\*/)(\s*)`)
	pythonBruinHeaderPattern = regexp.MustCompile(`(?s)\A(\s*(?:"""\s*@bruin.*?@bruin\s*"""|'''\s*@bruin.*?@bruin\s*'''|#\s*@bruin\s*\n.*?\n\s*#\s*@bruin\s*))(\s*)`)
)

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
				Usage: "directory for static web assets",
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
				patchTimers:        make(map[string]*time.Timer),
				hub:                events.NewDebouncedHub(150 * time.Millisecond),
				runner:             service.NewRunner(absRoot),
				freshness:          freshness.New(),
				duckDBOps:          make(map[string]*sync.Mutex),
				recentServerWrites: make(map[string]time.Time),
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
	router.Get("/api/events", s.handleEvents)
	router.Get("/api/workspace", s.handleGetWorkspace)
	router.Get("/api/ingestr/suggestions", s.handleGetIngestrSuggestions)
	router.Post("/api/pipelines", s.handleCreatePipeline)
	router.Put("/api/pipelines", s.handleUpdatePipeline)
	router.Delete("/api/pipelines/{id}", s.handleDeletePipeline)
	router.Get("/api/pipelines/{id}/materialization", s.handleGetPipelineMaterialization)
	router.Post("/api/pipelines/{id}/materialize/stream", s.handleMaterializePipelineStream)
	router.Post("/api/pipelines/{id}/assets", s.handleCreateAsset)
	router.Put("/api/pipelines/{pipelineID}/assets/{assetID}", s.handleUpdateAsset)
	router.Post("/api/assets/{assetID}/fill-columns-from-db", s.handleFillColumnsFromDB)
	router.Get("/api/assets/{assetID}/columns/infer", s.handleInferAssetColumns)
	router.Put("/api/assets/{assetID}/columns", s.handleUpdateAssetColumns)
	router.Delete("/api/pipelines/{pipelineID}/assets/{assetID}", s.handleDeleteAsset)
	router.Get("/api/assets/{assetID}/inspect", s.handleInspectAsset)
	router.Post("/api/assets/{assetID}/materialize", s.handleMaterializeAsset)
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
	state, err := s.computeWorkspaceState(ctx)
	if err != nil {
		return err
	}

	state.Revision = s.revision.Add(1)
	s.setState(state)
	return nil
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

func (s *webServer) computeWorkspaceState(ctx context.Context) (workspaceState, error) {
	state := workspaceState{
		Pipelines:   make([]webPipeline, 0),
		Connections: map[string]string{},
		Errors:      make([]string, 0),
		UpdatedAt:   time.Now().UTC(),
		Metadata:    map[string][]string{},
	}

	configPath := s.resolveConfigFilePath()
	if _, err := os.Stat(configPath); err == nil {
		cfg, cfgErr := config.LoadOrCreate(afero.NewOsFs(), configPath)
		if cfgErr == nil {
			state.SelectedEnvironment = cfg.SelectedEnvironmentName
			if cfg.SelectedEnvironment != nil && cfg.SelectedEnvironment.Connections != nil {
				state.Connections = cfg.SelectedEnvironment.Connections.ConnectionsSummaryList()
			}
		} else {
			state.Errors = append(state.Errors, fmt.Sprintf("config parse error: %v", cfgErr))
		}
	}

	pipelinePaths, err := bruinpath.GetPipelinePaths(s.workspaceRoot, PipelineDefinitionFiles)
	if err != nil {
		return state, err
	}

	builder := s.newPipelineBuilder()

	sort.Strings(pipelinePaths)
	for _, pPath := range pipelinePaths {
		parsed, parseErr := builder.CreatePipelineFromPath(ctx, pPath, pipeline.WithMutate())
		if parseErr != nil {
			state.Errors = append(state.Errors, fmt.Sprintf("%s: %v", pPath, parseErr))
			continue
		}

		relPipelinePath, relErr := filepath.Rel(s.workspaceRoot, pPath)
		if relErr != nil {
			relPipelinePath = pPath
		}

		pSummary := webPipeline{
			ID:     encodeID(relPipelinePath),
			Name:   parsed.Name,
			Path:   filepath.ToSlash(relPipelinePath),
			Assets: make([]webAsset, 0, len(parsed.Assets)),
		}

		if pSummary.Name == "" {
			pSummary.Name = filepath.Base(pPath)
		}

		for _, asset := range parsed.Assets {
			assetPath := asset.ExecutableFile.Path
			if assetPath == "" {
				assetPath = asset.DefinitionFile.Path
			}

			relAssetPath, relErr := filepath.Rel(s.workspaceRoot, assetPath)
			if relErr != nil {
				relAssetPath = assetPath
			}

			upstreams := make([]string, 0, len(asset.Upstreams))
			for _, up := range asset.Upstreams {
				upstreams = append(upstreams, up.Value)
			}

			connectionName := ""
			if conn, connErr := parsed.GetConnectionNameForAsset(asset); connErr == nil {
				connectionName = conn
			}

			declaredMatType := string(asset.Materialization.Type)

			pSummary.Assets = append(pSummary.Assets, webAsset{
				ID:                  encodeID(filepath.ToSlash(relAssetPath)),
				Name:                asset.Name,
				Type:                string(asset.Type),
				Path:                filepath.ToSlash(relAssetPath),
				Content:             asset.ExecutableFile.Content,
				Upstreams:           upstreams,
				Meta:                asset.Meta,
				Columns:             pipelineColumnsToWebColumns(asset.Columns),
				Connection:          connectionName,
				MaterializationType: declaredMatType,
				IsMaterialized:      false,
			})
		}

		state.Pipelines = append(state.Pipelines, pSummary)
	}

	state.Metadata["pipeline_definition_files"] = PipelineDefinitionFiles
	state.Metadata["asset_directories"] = assetsDirectoryNames

	return state, nil
}

func (s *webServer) resolveConfigFilePath() string {
	repoRoot, err := git.FindRepoFromPath(s.workspaceRoot)
	if err == nil && repoRoot != nil && strings.TrimSpace(repoRoot.Path) != "" {
		return filepath.Join(repoRoot.Path, ".bruin.yml")
	}

	return filepath.Join(s.workspaceRoot, ".bruin.yml")
}

func (s *webServer) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (s *webServer) writeAPIError(w http.ResponseWriter, status int, code, message string) {
	if strings.TrimSpace(message) == "" {
		message = http.StatusText(status)
	}

	s.writeJSON(w, status, map[string]any{
		"status": "error",
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}

func writeSSEJSON(w http.ResponseWriter, flusher http.Flusher, event string, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}

	if event != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", event); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
		return err
	}

	flusher.Flush()
	return nil
}

func (s *webServer) handleGetWorkspace(w http.ResponseWriter, _ *http.Request) {
	s.writeJSON(w, http.StatusOK, s.currentState())
}

func (s *webServer) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		webapi.WriteInternalError(w, "streaming_unsupported", "streaming unsupported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := s.hub.Subscribe()
	defer s.hub.Unsubscribe(ch)

	initial := workspaceEvent{
		Type:      "workspace.updated",
		Workspace: stripAssetContent(s.currentState()),
		Lite:      true,
	}
	if payload, err := json.Marshal(initial); err == nil {
		_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
		flusher.Flush()
	}

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:
			_, _ = fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

type createPipelineRequest struct {
	Path    string `json:"path"`
	Name    string `json:"name"`
	Content string `json:"content"`
}

func (s *webServer) handleCreatePipeline(w http.ResponseWriter, r *http.Request) {
	var req createPipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeAPIError(w, http.StatusBadRequest, "invalid_request_body", err.Error())
		return
	}

	if req.Path == "" {
		s.writeAPIError(w, http.StatusBadRequest, "pipeline_path_required", "path is required")
		return
	}

	absPath, err := safeJoin(s.workspaceRoot, req.Path)
	if err != nil {
		s.writeAPIError(w, http.StatusBadRequest, "invalid_pipeline_path", err.Error())
		return
	}

	if err := os.MkdirAll(absPath, 0o755); err != nil {
		s.writeAPIError(w, http.StatusInternalServerError, "pipeline_create_failed", err.Error())
		return
	}

	content := req.Content
	if strings.TrimSpace(content) == "" {
		name := req.Name
		if name == "" {
			name = filepath.Base(absPath)
		}
		content = fmt.Sprintf("name: %s\n", name)
	}

	if err := os.WriteFile(filepath.Join(absPath, "pipeline.yml"), []byte(content), 0o644); err != nil {
		s.writeAPIError(w, http.StatusInternalServerError, "pipeline_write_failed", err.Error())
		return
	}

	s.suppressWatcherFor(req.Path)
	s.pushWorkspaceUpdateImmediate(r.Context(), "pipeline.created", req.Path)
	s.writeJSON(w, http.StatusCreated, map[string]string{"status": "ok"})
}

type updatePipelineRequest struct {
	ID      string `json:"id"`
	Content string `json:"content"`
}

func (s *webServer) handleUpdatePipeline(w http.ResponseWriter, r *http.Request) {
	var req updatePipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		webapi.WriteBadRequest(w, "invalid_request_body", err.Error())
		return
	}

	relPath, err := decodeID(req.ID)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_pipeline_id", "invalid pipeline id")
		return
	}

	absPath, err := safeJoin(s.workspaceRoot, relPath)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_pipeline_path", err.Error())
		return
	}

	if err := os.WriteFile(filepath.Join(absPath, "pipeline.yml"), []byte(req.Content), 0o644); err != nil {
		webapi.WriteInternalError(w, "pipeline_write_failed", err.Error())
		return
	}

	s.suppressWatcherFor(relPath)
	s.pushWorkspaceUpdateImmediate(r.Context(), "pipeline.updated", relPath)
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *webServer) handleDeletePipeline(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	relPath, err := decodeID(id)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_pipeline_id", "invalid pipeline id")
		return
	}

	absPath, err := safeJoin(s.workspaceRoot, relPath)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_pipeline_path", err.Error())
		return
	}

	if err := os.RemoveAll(absPath); err != nil {
		webapi.WriteInternalError(w, "pipeline_delete_failed", err.Error())
		return
	}

	s.suppressWatcherFor(relPath)
	s.pushWorkspaceUpdateImmediate(r.Context(), "pipeline.deleted", relPath)
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *webServer) handleGetPipelineMaterialization(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "id")
	relPipelinePath, err := decodeID(pipelineID)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_pipeline_id", "invalid pipeline id")
		return
	}

	absPipelinePath, err := safeJoin(s.workspaceRoot, relPipelinePath)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_pipeline_path", err.Error())
		return
	}

	builder := s.newPipelineBuilder()
	parsed, err := builder.CreatePipelineFromPath(r.Context(), absPipelinePath, pipeline.WithMutate())
	if err != nil {
		webapi.WriteBadRequest(w, "pipeline_parse_failed", err.Error())
		return
	}

	matInfo := s.inspectPipelineMaterializations(r.Context(), parsed)
	freshnessByAssetName := computePipelineFreshness(parsed, matInfo, s.freshness.GetAll())
	assets := make([]assetMaterializationState, 0, len(parsed.Assets))

	for _, asset := range parsed.Assets {
		assetPath := asset.ExecutableFile.Path
		if assetPath == "" {
			assetPath = asset.DefinitionFile.Path
		}

		relAssetPath, relErr := filepath.Rel(s.workspaceRoot, assetPath)
		if relErr != nil {
			relAssetPath = assetPath
		}

		connectionName := ""
		if conn, connErr := parsed.GetConnectionNameForAsset(asset); connErr == nil {
			connectionName = conn
		}

		key := materializationAssetKey(asset.Name, connectionName)
		item := assetMaterializationState{
			AssetID:         encodeID(filepath.ToSlash(relAssetPath)),
			Connection:      connectionName,
			DeclaredMatType: string(asset.Materialization.Type),
		}

		if info, ok := matInfo[key]; ok {
			item.IsMaterialized = info.IsMaterialized
			item.MaterializedAs = info.MaterializedAs
			item.FreshnessStatus = info.FreshnessStatus
			item.RowCount = info.RowCount
			if info.DeclaredMatType != "" {
				item.DeclaredMatType = info.DeclaredMatType
			}
		}

		if status, ok := freshnessByAssetName[asset.Name]; ok {
			item.FreshnessStatus = status
		}

		assets = append(assets, item)
	}

	s.writeJSON(w, http.StatusOK, pipelineMaterializationResponse{
		PipelineID: pipelineID,
		Assets:     assets,
	})
}

type createAssetRequest struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Path          string `json:"path"`
	Content       string `json:"content"`
	SourceAssetID string `json:"source_asset_id"`
}

func (s *webServer) handleCreateAsset(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "id")
	relPipelinePath, err := decodeID(pipelineID)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_pipeline_id", "invalid pipeline id")
		return
	}

	var req createAssetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		webapi.WriteBadRequest(w, "invalid_request_body", err.Error())
		return
	}

	if req.Name == "" && req.Path == "" && req.SourceAssetID == "" {
		webapi.WriteBadRequest(w, "missing_name_or_path", "name or path is required")
		return
	}

	pipelinePath, err := safeJoin(s.workspaceRoot, relPipelinePath)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_pipeline_path", err.Error())
		return
	}

	var sourceAsset *pipeline.Asset
	var sourcePipeline *pipeline.Pipeline
	var sourceConnectionName string
	var sourceRelAssetPath string

	if strings.TrimSpace(req.SourceAssetID) != "" {
		resolvedRelPath, resolvedPipeline, resolvedAsset, resolveErr := s.resolveAssetByID(r.Context(), req.SourceAssetID)
		if resolveErr != nil {
			webapi.WriteBadRequest(w, "invalid_source_asset_id", resolveErr.Error())
			return
		}

		if !pipelinePathsReferToSameRoot(resolvedPipeline.DefinitionFile.Path, pipelinePath) {
			webapi.WriteBadRequest(w, "invalid_source_asset", "source asset must belong to the selected pipeline")
			return
		}

		sourceAsset = resolvedAsset
		sourcePipeline = resolvedPipeline
		sourceRelAssetPath = resolvedRelPath
		if conn, connErr := sourcePipeline.GetConnectionNameForAsset(sourceAsset); connErr == nil {
			sourceConnectionName = conn
		}
	}

	assetName := strings.TrimSpace(req.Name)
	if assetName == "" && sourceAsset != nil {
		assetName = deriveDownstreamAssetName(sourceAsset.Name, sourcePipeline)
	}

	relAssetPath := req.Path
	if relAssetPath == "" {
		if sourceAsset != nil {
			sourceAbsAssetPath, pathErr := safeJoin(s.workspaceRoot, sourceRelAssetPath)
			if pathErr != nil {
				webapi.WriteBadRequest(w, "invalid_source_asset_path", pathErr.Error())
				return
			}

			sourcePipelineRelativeDir, relErr := filepath.Rel(pipelinePath, filepath.Dir(sourceAbsAssetPath))
			if relErr != nil {
				sourcePipelineRelativeDir = "assets"
			}

			assetTypeForPath := strings.TrimSpace(req.Type)
			if assetTypeForPath == "" {
				assetTypeForPath = deriveSQLAssetTypeForSource(sourceAsset, sourcePipeline, sourceConnectionName)
			}

			relAssetPath = filepath.ToSlash(filepath.Join(sourcePipelineRelativeDir, slug(assetName)+extensionForAssetType(assetTypeForPath)))
		} else {
			relAssetPath = filepath.ToSlash(filepath.Join("assets", slug(assetName)+extensionForAssetType(req.Type)))
		}
	}

	absAssetPath, err := safeJoin(pipelinePath, relAssetPath)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_asset_path", err.Error())
		return
	}

	if err := os.MkdirAll(filepath.Dir(absAssetPath), 0o755); err != nil {
		webapi.WriteInternalError(w, "asset_dir_create_failed", err.Error())
		return
	}

	assetType := strings.TrimSpace(req.Type)
	if assetType == "" {
		if sourceAsset != nil {
			assetType = deriveSQLAssetTypeForSource(sourceAsset, sourcePipeline, sourceConnectionName)
		} else {
			assetType = inferAssetTypeFromPath(relAssetPath)
		}
	}

	content := req.Content
	if content == "" {
		if assetName == "" {
			assetName = strings.TrimSuffix(filepath.Base(relAssetPath), filepath.Ext(relAssetPath))
		}

		if sourceAsset != nil {
			content = defaultDerivedSQLAssetContent(assetName, assetType, relAssetPath, sourceAsset.Name, sourceConnectionName)
		} else {
			content = defaultAssetContent(assetName, assetType, relAssetPath)
		}
	}

	if err := os.WriteFile(absAssetPath, []byte(content), 0o644); err != nil {
		webapi.WriteInternalError(w, "asset_write_failed", err.Error())
		return
	}

	if err := ensurePythonRequirementsFile(absAssetPath, assetType, relAssetPath); err != nil {
		webapi.WriteInternalError(w, "requirements_write_failed", err.Error())
		return
	}

	relWorkspaceAssetPath, _ := filepath.Rel(s.workspaceRoot, absAssetPath)
	assetPath := filepath.ToSlash(relWorkspaceAssetPath)
	s.suppressWatcherFor(assetPath)
	s.pushWorkspaceUpdateImmediate(r.Context(), "asset.created", assetPath)
	s.writeJSON(w, http.StatusCreated, map[string]string{
		"status":     "ok",
		"asset_id":   encodeID(assetPath),
		"asset_path": assetPath,
	})
}

type updateAssetRequest struct {
	Name                *string           `json:"name,omitempty"`
	Content             *string           `json:"content,omitempty"`
	MaterializationType *string           `json:"materialization_type,omitempty"`
	Meta                map[string]string `json:"meta,omitempty"`
}

type updateAssetColumnsRequest struct {
	Columns []webColumn `json:"columns"`
}

func (s *webServer) handleUpdateAsset(w http.ResponseWriter, r *http.Request) {
	assetID := chi.URLParam(r, "assetID")
	relAssetPath, err := decodeID(assetID)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_asset_id", "invalid asset id")
		return
	}

	var req updateAssetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		webapi.WriteBadRequest(w, "invalid_request_body", err.Error())
		return
	}

	absAssetPath, err := safeJoin(s.workspaceRoot, relAssetPath)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_asset_path", err.Error())
		return
	}

	originalBytes, err := os.ReadFile(absAssetPath)
	if err != nil {
		webapi.WriteInternalError(w, "asset_read_failed", err.Error())
		return
	}
	originalContent := string(originalBytes)
	desiredExecutable := extractExecutableContent(originalContent)
	if req.Content != nil {
		desiredExecutable = *req.Content
	}

	changedAssetIDs := []string{assetID}
	changedAssetPaths := []string{filepath.ToSlash(relAssetPath)}

	if req.Name != nil || req.MaterializationType != nil || req.Meta != nil {
		_, parsedPipeline, asset, resolveErr := s.resolveAssetByID(r.Context(), assetID)
		if resolveErr != nil {
			webapi.WriteBadRequest(w, "asset_resolve_failed", resolveErr.Error())
			return
		}

		originalAssetName := asset.Name
		renamedAsset := false

		if req.Name != nil {
			nextName := strings.TrimSpace(*req.Name)
			if nextName == "" {
				webapi.WriteBadRequest(w, "invalid_asset_name", "asset name cannot be empty")
				return
			}

			if existing := parsedPipeline.GetAssetByNameCaseInsensitive(nextName); existing != nil && existing.DefinitionFile.Path != asset.DefinitionFile.Path {
				webapi.WriteBadRequest(w, "duplicate_asset_name", fmt.Sprintf("an asset named %q already exists", nextName))
				return
			}

			if nextName != asset.Name {
				asset.Name = nextName
				renamedAsset = true
			}
		}

		if req.MaterializationType != nil {
			asset.Materialization.Type = pipeline.MaterializationType(strings.ToLower(strings.TrimSpace(*req.MaterializationType)))
		}

		if req.Meta != nil {
			nextMeta := make(map[string]string)
			for rawKey, rawValue := range req.Meta {
				key := strings.TrimSpace(rawKey)
				if key == "" {
					continue
				}

				nextMeta[key] = rawValue
			}

			if len(nextMeta) == 0 {
				asset.Meta = nil
			} else {
				asset.Meta = nextMeta
			}
		}

		if err := asset.Persist(afero.NewOsFs(), parsedPipeline); err != nil {
			webapi.WriteInternalError(w, "asset_persist_failed", err.Error())
			return
		}

		if renamedAsset {
			affectedIDs, affectedPaths, refactorErr := s.refactorDirectAssetDependencies(r.Context(), parsedPipeline, originalAssetName, asset.Name)
			if refactorErr != nil {
				webapi.WriteInternalError(w, "asset_rename_refactor_failed", refactorErr.Error())
				return
			}

			changedAssetIDs = appendUniqueStrings(changedAssetIDs, affectedIDs...)
			changedAssetPaths = appendUniqueStrings(changedAssetPaths, affectedPaths...)
		}
	}

	currentBytes, err := os.ReadFile(absAssetPath)
	if err != nil {
		webapi.WriteInternalError(w, "asset_read_failed", err.Error())
		return
	}

	mergedContent := mergeExecutableContent(string(currentBytes), desiredExecutable)
	if err := os.WriteFile(absAssetPath, []byte(mergedContent), 0o644); err != nil {
		webapi.WriteInternalError(w, "asset_write_failed", err.Error())
		return
	}

	if req.Content != nil && strings.HasSuffix(strings.ToLower(relAssetPath), ".sql") {
		s.scheduleSQLAssetPatches(relAssetPath)
	}

	for _, changedPath := range changedAssetPaths {
		s.suppressWatcherFor(changedPath)
	}
	s.pushWorkspaceUpdateImmediateWithChangedIDs(r.Context(), "asset.updated", relAssetPath, changedAssetIDs)
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *webServer) refactorDirectAssetDependencies(ctx context.Context, parsedPipeline *pipeline.Pipeline, oldName, newName string) ([]string, []string, error) {
	if parsedPipeline == nil || strings.TrimSpace(oldName) == strings.TrimSpace(newName) {
		return nil, nil, nil
	}

	sqlParserInstance, err := sqlparser.NewSQLParser(false)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create sql parser: %w", err)
	}
	defer sqlParserInstance.Close()

	renderer := jinja.NewRendererWithYesterday(parsedPipeline.Name, "web-rename")
	fs := afero.NewOsFs()
	changedIDs := make([]string, 0)
	changedPaths := make([]string, 0)

	for _, current := range parsedPipeline.Assets {
		if strings.EqualFold(current.Name, newName) || strings.EqualFold(current.Name, oldName) {
			continue
		}

		updated := false
		for index, upstream := range current.Upstreams {
			if !strings.EqualFold(upstream.Value, oldName) {
				continue
			}

			current.Upstreams[index].Value = newName
			updated = true
		}

		isSQLAsset := isSQLAssetFile(current)
		if isSQLAsset {
			nextContent := replaceAssetNameReferences(current.ExecutableFile.Content, oldName, newName)
			if nextContent != current.ExecutableFile.Content {
				current.ExecutableFile.Content = nextContent
				updated = true
			}
		}

		if !updated {
			continue
		}

		if err := current.Persist(fs, parsedPipeline); err != nil {
			return nil, nil, fmt.Errorf("failed to persist renamed dependency updates for asset '%s': %w", current.Name, err)
		}

		if isSQLAsset {
			if err := updateAssetDependencies(ctx, current, parsedPipeline, sqlParserInstance, renderer); err != nil {
				return nil, nil, fmt.Errorf("failed to refresh dependencies for asset '%s': %w", current.Name, err)
			}
		}

		assetPath := current.ExecutableFile.Path
		if assetPath == "" {
			assetPath = current.DefinitionFile.Path
		}

		relAssetPath, relErr := filepath.Rel(s.workspaceRoot, assetPath)
		if relErr != nil {
			relAssetPath = assetPath
		}

		normalizedPath := filepath.ToSlash(relAssetPath)
		changedIDs = append(changedIDs, encodeID(normalizedPath))
		changedPaths = append(changedPaths, normalizedPath)
	}

	return changedIDs, changedPaths, nil
}

func isSQLAssetFile(asset *pipeline.Asset) bool {
	if asset == nil {
		return false
	}

	assetPath := asset.ExecutableFile.Path
	if assetPath == "" {
		assetPath = asset.DefinitionFile.Path
	}
	assetPath = strings.ToLower(assetPath)
	assetType := strings.ToLower(string(asset.Type))
	return strings.HasSuffix(assetPath, ".sql") || strings.Contains(assetType, "sql")
}

func replaceAssetNameReferences(content, oldName, newName string) string {
	trimmedOld := strings.TrimSpace(oldName)
	trimmedNew := strings.TrimSpace(newName)
	if trimmedOld == "" || trimmedNew == "" || trimmedOld == trimmedNew {
		return content
	}

	pattern := fmt.Sprintf(`(?i)(^|[^A-Za-z0-9_.])(%s)([^A-Za-z0-9_.]|$)`, regexp.QuoteMeta(trimmedOld))
	re := regexp.MustCompile(pattern)
	return re.ReplaceAllString(content, `${1}`+trimmedNew+`${3}`)
}

func appendUniqueStrings(values []string, extras ...string) []string {
	seen := make(map[string]struct{}, len(values)+len(extras))
	for _, value := range values {
		seen[value] = struct{}{}
	}

	for _, extra := range extras {
		if extra == "" {
			continue
		}
		if _, ok := seen[extra]; ok {
			continue
		}
		seen[extra] = struct{}{}
		values = append(values, extra)
	}

	return values
}

func (s *webServer) scheduleSQLAssetPatches(relAssetPath string) {
	assetPath := filepath.ToSlash(relAssetPath)

	s.patchMu.Lock()
	if existing, ok := s.patchTimers[assetPath]; ok {
		existing.Stop()
	}

	s.patchTimers[assetPath] = time.AfterFunc(1500*time.Millisecond, func() {
		s.runSQLAssetPatches(assetPath)

		s.patchMu.Lock()
		delete(s.patchTimers, assetPath)
		s.patchMu.Unlock()
	})
	s.patchMu.Unlock()
}

func (s *webServer) runSQLAssetPatches(relAssetPath string) {
	prefixedPath := relAssetPath
	if !strings.HasPrefix(prefixedPath, ".") {
		prefixedPath = "." + prefixedPath
	}

	commands := [][]string{
		{"patch", "fill-columns-from-db", prefixedPath},
		{"patch", "fill-asset-dependencies", relAssetPath},
	}

	for _, args := range commands {
		_, _ = s.runner.Run(context.Background(), args)
	}

	s.suppressWatcherFor(relAssetPath)
	s.pushWorkspaceUpdate(context.Background(), "asset.patched", relAssetPath)
}

func (s *webServer) handleFillColumnsFromDB(w http.ResponseWriter, r *http.Request) {
	assetID := chi.URLParam(r, "assetID")
	relAssetPath, _, asset, err := s.resolveAssetByID(r.Context(), assetID)
	if err != nil {
		webapi.WriteBadRequest(w, "asset_resolve_failed", err.Error())
		return
	}

	assetType := strings.ToLower(string(asset.Type))
	if !strings.Contains(assetType, "sql") && !strings.HasSuffix(strings.ToLower(relAssetPath), ".sql") {
		webapi.WriteBadRequest(w, "unsupported_asset_type", "fill-columns-from-db is supported for sql assets only")
		return
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
		out, runErr := s.runner.Run(r.Context(), args)

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
	s.pushWorkspaceUpdateImmediate(r.Context(), "asset.updated", relAssetPath)

	status := http.StatusOK
	responseStatus := "ok"
	if !allSucceeded {
		status = http.StatusBadRequest
		responseStatus = "error"
	}

	s.writeJSON(w, status, map[string]any{
		"status":  responseStatus,
		"results": results,
	})
}

func (s *webServer) handleInferAssetColumns(w http.ResponseWriter, r *http.Request) {
	assetID := chi.URLParam(r, "assetID")
	relAssetPath, _, _, err := s.resolveAssetByID(r.Context(), assetID)
	if err != nil {
		webapi.WriteBadRequest(w, "asset_resolve_failed", err.Error())
		return
	}

	cmdArgs := []string{"query", "--asset", relAssetPath, "--output", "json", "--limit", "1"}
	output, err := s.runner.Run(r.Context(), cmdArgs)
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]any{
			"status":     "error",
			"columns":    []webColumn{},
			"raw_output": string(output),
			"command":    cmdArgs,
			"error":      err.Error(),
		})
		return
	}

	inferred := inferWebColumnsFromQueryOutput(output)
	s.writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"columns":    inferred,
		"raw_output": string(output),
		"command":    cmdArgs,
	})
}

func (s *webServer) handleUpdateAssetColumns(w http.ResponseWriter, r *http.Request) {
	assetID := chi.URLParam(r, "assetID")
	_, parsedPipeline, asset, err := s.resolveAssetByID(r.Context(), assetID)
	if err != nil {
		webapi.WriteBadRequest(w, "asset_resolve_failed", err.Error())
		return
	}

	var req updateAssetColumnsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		webapi.WriteBadRequest(w, "invalid_request_body", err.Error())
		return
	}

	asset.Columns = webColumnsToPipelineColumns(req.Columns)
	err = asset.Persist(afero.NewOsFs(), parsedPipeline)
	if err != nil {
		webapi.WriteInternalError(w, "asset_persist_failed", err.Error())
		return
	}

	relAssetPath, decodeErr := decodeID(assetID)
	if decodeErr == nil {
		s.suppressWatcherFor(relAssetPath)
		s.pushWorkspaceUpdateImmediate(r.Context(), "asset.columns.updated", relAssetPath)
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *webServer) handleDeleteAsset(w http.ResponseWriter, r *http.Request) {
	assetID := chi.URLParam(r, "assetID")
	relAssetPath, err := decodeID(assetID)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_asset_id", "invalid asset id")
		return
	}

	absAssetPath, err := safeJoin(s.workspaceRoot, relAssetPath)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_asset_path", err.Error())
		return
	}

	if err := os.Remove(absAssetPath); err != nil {
		webapi.WriteInternalError(w, "asset_delete_failed", err.Error())
		return
	}

	s.suppressWatcherFor(relAssetPath)
	s.pushWorkspaceUpdateImmediate(r.Context(), "asset.deleted", relAssetPath)
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *webServer) resolveAssetByID(ctx context.Context, assetID string) (string, *pipeline.Pipeline, *pipeline.Asset, error) {
	relAssetPath, err := decodeID(assetID)
	if err != nil {
		return "", nil, nil, fmt.Errorf("invalid asset id")
	}

	absAssetPath, err := safeJoin(s.workspaceRoot, relAssetPath)
	if err != nil {
		return "", nil, nil, err
	}

	pipelinePath, err := bruinpath.GetPipelineRootFromTask(absAssetPath, PipelineDefinitionFiles)
	if err != nil {
		return "", nil, nil, err
	}

	builder := s.newPipelineBuilder()
	parsed, err := builder.CreatePipelineFromPath(ctx, pipelinePath, pipeline.WithMutate())
	if err != nil {
		return "", nil, nil, err
	}

	normalizedTarget := filepath.ToSlash(relAssetPath)
	for _, current := range parsed.Assets {
		assetPath := current.ExecutableFile.Path
		if assetPath == "" {
			assetPath = current.DefinitionFile.Path
		}

		relCurrent, relErr := filepath.Rel(s.workspaceRoot, assetPath)
		if relErr != nil {
			continue
		}

		if filepath.ToSlash(relCurrent) == normalizedTarget {
			return normalizedTarget, parsed, current, nil
		}
	}

	return "", nil, nil, fmt.Errorf("asset not found in pipeline")
}

type duckDBExecutionInfo struct {
	ConnectionName string
	DatabasePath   string
	LockKey        string
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

func (s *webServer) findDuckDBExecutionInfoByAsset(ctx context.Context, assetID string) (*duckDBExecutionInfo, error) {
	_, parsed, asset, err := s.resolveAssetByID(ctx, assetID)
	if err != nil {
		return nil, err
	}

	connectionName, err := parsed.GetConnectionNameForAsset(asset)
	if err != nil || connectionName == "" {
		return nil, nil
	}

	configPath := s.resolveConfigFilePath()
	if _, statErr := os.Stat(configPath); statErr != nil {
		return nil, nil
	}

	cfg, cfgErr := config.LoadOrCreate(afero.NewOsFs(), configPath)
	if cfgErr != nil || cfg.SelectedEnvironment == nil || cfg.SelectedEnvironment.Connections == nil {
		return nil, nil
	}

	for _, conn := range cfg.SelectedEnvironment.Connections.DuckDB {
		if conn.Name != connectionName {
			continue
		}

		databasePath := strings.TrimSpace(conn.Path)
		if databasePath == "" {
			databasePath = connectionName
		} else {
			databasePath = filepath.Clean(databasePath)
		}

		return &duckDBExecutionInfo{
			ConnectionName: connectionName,
			DatabasePath:   databasePath,
			LockKey:        "duckdb:" + databasePath,
		}, nil
	}

	return nil, nil
}

func (s *webServer) buildReadOnlyConfigFile(
	duckDBInfo *duckDBExecutionInfo,
) (string, func(), error) {
	if duckDBInfo == nil || duckDBInfo.ConnectionName == "" {
		return "", nil, fmt.Errorf("duckdb read-only config requires connection info")
	}

	configPath := s.resolveConfigFilePath()
	cfg, err := config.LoadOrCreate(afero.NewOsFs(), configPath)
	if err != nil {
		return "", nil, err
	}

	if cfg.SelectedEnvironment == nil || cfg.SelectedEnvironment.Connections == nil {
		return "", nil, fmt.Errorf("selected environment has no connections")
	}

	envName := cfg.SelectedEnvironmentName
	if envName == "" {
		envName = cfg.DefaultEnvironmentName
	}

	env, ok := cfg.Environments[envName]
	if !ok || env.Connections == nil {
		return "", nil, fmt.Errorf("environment '%s' not found", envName)
	}

	found := false
	for i := range env.Connections.DuckDB {
		if env.Connections.DuckDB[i].Name != duckDBInfo.ConnectionName {
			continue
		}

		env.Connections.DuckDB[i].Path = service.AppendDuckDBReadOnlyMode(env.Connections.DuckDB[i].Path)
		found = true
		break
	}

	if !found {
		return "", nil, fmt.Errorf("duckdb connection '%s' not found", duckDBInfo.ConnectionName)
	}

	cfg.Environments[envName] = env

	tempFile, err := os.CreateTemp("", "bruin-web-readonly-*.yml")
	if err != nil {
		return "", nil, err
	}

	cleanup := func() {
		_ = os.Remove(tempFile.Name())
	}

	if err := tempFile.Close(); err != nil {
		cleanup()
		return "", nil, err
	}

	content, err := yaml.Marshal(cfg)
	if err != nil {
		cleanup()
		return "", nil, err
	}

	if err := os.WriteFile(tempFile.Name(), content, 0o600); err != nil {
		cleanup()
		return "", nil, err
	}

	return tempFile.Name(), cleanup, nil
}

func (s *webServer) handleMaterializeAsset(w http.ResponseWriter, r *http.Request) {
	assetID := chi.URLParam(r, "assetID")
	relAssetPath, err := decodeID(assetID)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_asset_id", "invalid asset id")
		return
	}

	duckDBInfo, infoErr := s.findDuckDBExecutionInfoByAsset(r.Context(), assetID)
	if infoErr != nil {
		webapi.WriteBadRequest(w, "duckdb_info_failed", infoErr.Error())
		return
	}

	cmdArgs := []string{"run", relAssetPath}

	var output []byte
	var attempts int
	run := func() {
		output, err, attempts = s.runner.RunWithRetry(r.Context(), cmdArgs, 4, 200*time.Millisecond)
	}

	if duckDBInfo != nil {
		mu := s.getDuckDBOperationMutex(duckDBInfo.LockKey)
		mu.Lock()
		run()
		mu.Unlock()
	} else {
		run()
	}

	if err != nil {
		statusCode := http.StatusBadRequest
		errorMessage := err.Error()
		if service.IsDuckDBLockError(err, output) {
			statusCode = http.StatusConflict
			errorMessage = "duckdb database is busy (lock held by another process), please retry"
		}

		s.writeJSON(w, statusCode, map[string]any{
			"status":    "error",
			"command":   cmdArgs,
			"output":    string(output),
			"error":     errorMessage,
			"exit_code": 1,
			"attempts":  attempts,
			"retryable": statusCode == http.StatusConflict,
		})
		return
	}

	// Record successful materialization and compute affected downstream assets.
	// For inspect refresh: the materialized asset + direct (1-level) downstreams.
	// Transitive downstreams still read from un-materialized intermediate tables.
	materializedAt := time.Now().UTC()
	if assetName := s.findAssetNameByID(assetID); assetName != "" {
		s.freshness.RecordMaterialization(assetName, materializedAt, "succeeded")
	}
	affected := s.findMaterializationInspectIDs(assetID)

	s.writeJSON(w, http.StatusOK, map[string]any{
		"status":            "ok",
		"command":           cmdArgs,
		"output":            string(output),
		"exit_code":         0,
		"attempts":          attempts,
		"materialized_at":   materializedAt,
		"changed_asset_ids": affected,
	})
}

func (s *webServer) handleInspectAsset(w http.ResponseWriter, r *http.Request) {
	assetID := chi.URLParam(r, "assetID")
	relAssetPath, err := decodeID(assetID)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_asset_id", "invalid asset id")
		return
	}

	limit := r.URL.Query().Get("limit")
	if limit == "" {
		limit = "200"
	}

	environment := r.URL.Query().Get("environment")
	duckDBInfo, infoErr := s.findDuckDBExecutionInfoByAsset(r.Context(), assetID)
	if infoErr != nil {
		webapi.WriteBadRequest(w, "duckdb_info_failed", infoErr.Error())
		return
	}

	cmdArgs := []string{"query", "--asset", relAssetPath, "--output", "json", "--limit", limit}
	if environment != "" {
		cmdArgs = append(cmdArgs, "--environment", environment)
	}

	var output []byte
	var attempts int
	run := func(args []string) {
		output, err, attempts = s.runner.RunWithRetry(r.Context(), args, 4, 150*time.Millisecond)
	}

	if duckDBInfo != nil {
		mu := s.getDuckDBOperationMutex(duckDBInfo.LockKey)
		mu.Lock()
		run(cmdArgs)

		if err != nil && service.IsDuckDBLockError(err, output) {
			if readOnlyConfigPath, cleanup, cfgErr := s.buildReadOnlyConfigFile(duckDBInfo); cfgErr == nil {
				defer cleanup()
				readOnlyArgs := append([]string{}, cmdArgs...)
				readOnlyArgs = append(readOnlyArgs, "--config-file", readOnlyConfigPath)
				run(readOnlyArgs)
				cmdArgs = readOnlyArgs
			}
		}
		mu.Unlock()
	} else {
		run(cmdArgs)
	}

	if err != nil {
		statusCode := http.StatusBadRequest
		errorMessage := err.Error()
		if service.IsDuckDBLockError(err, output) {
			statusCode = http.StatusConflict
			errorMessage = "duckdb database is busy (lock held by another process), please retry"
		}

		s.writeJSON(w, statusCode, map[string]any{
			"status":     "error",
			"columns":    []string{},
			"rows":       []map[string]any{},
			"raw_output": string(output),
			"command":    cmdArgs,
			"error":      errorMessage,
			"attempts":   attempts,
			"retryable":  statusCode == http.StatusConflict,
		})
		return
	}

	columns, rows := parseQueryJSONOutput(output)
	s.writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"columns":    columns,
		"rows":       rows,
		"raw_output": string(output),
		"command":    cmdArgs,
		"attempts":   attempts,
	})
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
		items, itemErr := buildS3SuggestionItems(r.Context(), s3Conn, prefix)
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
) ([]ingestrSuggestionItem, error) {
	normalizedPrefix := strings.TrimSpace(prefix)
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

	return limitSuggestionItems(suggestions, 200), nil
}

func limitSuggestionItems(items []ingestrSuggestionItem, max int) []ingestrSuggestionItem {
	if max <= 0 || len(items) <= max {
		return items
	}
	return items[:max]
}

func (s *webServer) handleMaterializePipelineStream(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "id")
	if strings.TrimSpace(pipelineID) == "" {
		webapi.WriteBadRequest(w, "invalid_pipeline_id", "invalid pipeline id")
		return
	}

	target, err := resolvePipelineRunTarget(pipelineID)
	if err != nil {
		webapi.WriteBadRequest(w, "invalid_pipeline_id", "invalid pipeline id")
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		webapi.WriteInternalError(w, "streaming_unsupported", "streaming unsupported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	cmdArgs := []string{"run", target}
	_ = writeSSEJSON(w, flusher, "start", map[string]any{
		"command": cmdArgs,
	})

	output, runErr := s.runner.Stream(r.Context(), cmdArgs, func(chunk []byte) {
		_ = writeSSEJSON(w, flusher, "output", map[string]any{
			"chunk": string(chunk),
		})
	})

	changedAssetIDs := make([]string, 0)
	var materializedAt *time.Time
	if runErr == nil {
		now := time.Now().UTC()
		materializedAt = &now
		state := s.currentState()
		for _, currentPipeline := range state.Pipelines {
			if currentPipeline.ID != pipelineID {
				continue
			}

			for _, asset := range currentPipeline.Assets {
				changedAssetIDs = append(changedAssetIDs, asset.ID)
				if strings.TrimSpace(asset.Name) != "" {
					s.freshness.RecordMaterialization(asset.Name, now, "succeeded")
				}
			}
			break
		}
	}

	status := "ok"
	errorMessage := ""
	exitCode := 0
	if runErr != nil {
		status = "error"
		errorMessage = runErr.Error()
		exitCode = 1
	}

	_ = writeSSEJSON(w, flusher, "done", map[string]any{
		"status":            status,
		"command":           cmdArgs,
		"output":            string(output),
		"error":             errorMessage,
		"exit_code":         exitCode,
		"changed_asset_ids": changedAssetIDs,
		"materialized_at":   materializedAt,
	})
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
	if stat, err := os.Stat(s.staticDir); err == nil && stat.IsDir() {
		fileServer := http.FileServer(http.Dir(s.staticDir))
		if r.URL.Path == "/" {
			indexPath := filepath.Join(s.staticDir, "index.html")
			if _, err := os.Stat(indexPath); err == nil {
				http.ServeFile(w, r, indexPath)
				return
			}
		}

		if _, err := os.Stat(filepath.Join(s.staticDir, strings.TrimPrefix(r.URL.Path, "/"))); err == nil {
			fileServer.ServeHTTP(w, r)
			return
		}

		indexPath := filepath.Join(s.staticDir, "index.html")
		if _, err := os.Stat(indexPath); err == nil {
			http.ServeFile(w, r, indexPath)
			return
		}
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Bruin Web backend is running. Build frontend assets into web/dist to serve the UI."))
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
	clean := filepath.Clean(filepath.FromSlash(relPath))
	if clean == "." || clean == "" {
		return root, nil
	}
	if filepath.IsAbs(clean) || strings.HasPrefix(clean, "..") {
		return "", fmt.Errorf("invalid path: %s", relPath)
	}
	return filepath.Join(root, clean), nil
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
	trimmed := strings.TrimSpace(strings.ToLower(input))
	if trimmed == "" {
		return "asset"
	}
	b := strings.Builder{}
	for _, r := range trimmed {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			continue
		}
		if r == '_' || r == '-' || r == ' ' || r == '.' {
			b.WriteRune('_')
		}
	}

	result := strings.Trim(b.String(), "-")
	if result == "" {
		return "asset"
	}
	return result
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
	if strings.HasSuffix(strings.ToLower(assetPath), ".py") {
		return fmt.Sprintf(
			`""" @bruin

name: %s
image: python:3.11
connection: duckdb-default

materialization:
  type: table

@bruin """

import pandas as pd


def materialize():
    items = 100000
    df = pd.DataFrame({
        'col1': range(items),
        'col2': [f'value_new_{i}' for i in range(items)],
        'col3': [i * 6.0 for i in range(items)]
    })

    return df
`, assetName)
	}
	if strings.HasSuffix(strings.ToLower(assetPath), ".sql") {
		return fmt.Sprintf("/* @bruin\n\nname: %s\ntype: %s\nmaterialization:\n  type: view\n\n@bruin */\n", assetName, assetType)
	}

	return fmt.Sprintf("/* @bruin\n\nname: %s\ntype: %s\n\n@bruin */\n", assetName, assetType)
}

func defaultDerivedSQLAssetContent(assetName, assetType, assetPath, sourceAssetName, connectionName string) string {
	if !strings.HasSuffix(strings.ToLower(assetPath), ".sql") {
		return defaultAssetContent(assetName, assetType, assetPath)
	}

	connectionSection := ""
	if strings.TrimSpace(connectionName) != "" {
		connectionSection = fmt.Sprintf("connection: %s\n", connectionName)
	}

	return fmt.Sprintf(
		"/* @bruin\n\nname: %s\ntype: %s\n%sdepends:\n  - %s\nmaterialization:\n  type: view\n\n@bruin */\n\nselect *\nfrom %s\n",
		assetName,
		assetType,
		connectionSection,
		sourceAssetName,
		quoteQualifiedIdentifier(sourceAssetName),
	)
}

func ensurePythonRequirementsFile(absAssetPath, assetType, relAssetPath string) error {
	lowerType := strings.ToLower(strings.TrimSpace(assetType))
	lowerPath := strings.ToLower(strings.TrimSpace(relAssetPath))
	if !strings.Contains(lowerType, "python") && !strings.HasSuffix(lowerPath, ".py") {
		return nil
	}

	requirementsPath := filepath.Join(filepath.Dir(absAssetPath), "requirements.txt")
	if _, err := os.Stat(requirementsPath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	return os.WriteFile(requirementsPath, []byte("pandas===3.0.1\n"), 0o644)
}

func splitBruinHeader(content string) (header string, separator string, body string, found bool) {
	if match := sqlBruinHeaderPattern.FindStringSubmatchIndex(content); match != nil {
		header = content[match[2]:match[3]]
		separator = content[match[4]:match[5]]
		body = content[match[1]:]
		return header, separator, body, true
	}

	if match := pythonBruinHeaderPattern.FindStringSubmatchIndex(content); match != nil {
		header = content[match[2]:match[3]]
		separator = content[match[4]:match[5]]
		body = content[match[1]:]
		return header, separator, body, true
	}

	return "", "", content, false
}

func extractExecutableContent(content string) string {
	_, _, body, found := splitBruinHeader(content)
	if !found {
		return content
	}
	return body
}

func mergeExecutableContent(currentFileContent, executableContent string) string {
	header, separator, _, found := splitBruinHeader(currentFileContent)
	if !found {
		return executableContent
	}

	sep := separator
	if sep == "" {
		sep = "\n\n"
	}

	return header + sep + strings.TrimLeft(executableContent, "\r\n")
}

func encodeID(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(filepath.ToSlash(value)))
}

func decodeID(value string) (string, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

func parseQueryJSONOutput(output []byte) ([]string, []map[string]any) {
	rows := make([]map[string]any, 0)

	var asRows []map[string]any
	if err := json.Unmarshal(output, &asRows); err == nil {
		rows = asRows
		return inferColumns(rows), rows
	}

	var asEnvelope map[string]any
	if err := json.Unmarshal(output, &asEnvelope); err == nil {
		columns := extractColumnNames(asEnvelope["columns"])

		if v, ok := asEnvelope["rows"]; ok {
			if parsedRows := castRows(v); len(parsedRows) > 0 {
				rows = parsedRows
			} else if parsedRowsByColumns := castRowsByColumns(v, columns); len(parsedRowsByColumns) > 0 {
				rows = parsedRowsByColumns
			}
		}
		if len(rows) == 0 {
			if v, ok := asEnvelope["data"]; ok {
				if parsedRows := castRows(v); len(parsedRows) > 0 {
					rows = parsedRows
				} else {
					rows = castRowsByColumns(v, columns)
				}
			}
		}

		if len(columns) == 0 {
			columns = inferColumns(rows)
		}

		return columns, rows
	}

	return []string{}, rows
}

func (s *webServer) inspectPipelineMaterializations(ctx context.Context, parsed *pipeline.Pipeline) map[string]pipelineMaterializationInfo {
	result := make(map[string]pipelineMaterializationInfo)

	assetsByConnection := make(map[string][]*pipeline.Asset)
	for _, asset := range parsed.Assets {
		conn, err := parsed.GetConnectionNameForAsset(asset)
		if err != nil || conn == "" {
			continue
		}
		assetsByConnection[conn] = append(assetsByConnection[conn], asset)
	}

	for connName, assets := range assetsByConnection {
		objects, err := s.fetchObjectsForConnection(ctx, connName)
		if err != nil || len(objects) == 0 {
			for _, asset := range assets {
				key := materializationAssetKey(asset.Name, connName)
				result[key] = pipelineMaterializationInfo{
					AssetName:       asset.Name,
					Connection:      connName,
					DeclaredMatType: string(asset.Materialization.Type),
				}
			}
			continue
		}

		wanted := make(map[string]struct{})
		for _, asset := range assets {
			wanted[normalizeIdentifier(asset.Name)] = struct{}{}
			parts := strings.Split(normalizeIdentifier(asset.Name), ".")
			if len(parts) > 1 {
				wanted[parts[len(parts)-1]] = struct{}{}
			}
		}

		candidateObjects := make([]dbObjectInfo, 0)
		for _, object := range objects {
			if _, ok := wanted[normalizeIdentifier(object.QualifiedName)]; ok {
				candidateObjects = append(candidateObjects, object)
				continue
			}
			if _, ok := wanted[normalizeIdentifier(object.Name)]; ok {
				candidateObjects = append(candidateObjects, object)
			}
		}

		tableObjects := make([]dbObjectInfo, 0, len(candidateObjects))
		for _, object := range candidateObjects {
			if object.Kind == "table" {
				tableObjects = append(tableObjects, object)
			}
		}

		rowCounts := s.fetchRowCountsForObjects(ctx, connName, tableObjects)

		objectsByName := make(map[string]dbObjectInfo)
		for _, object := range objects {
			objectsByName[normalizeIdentifier(object.QualifiedName)] = object
			objectsByName[normalizeIdentifier(object.Name)] = object
		}

		for _, asset := range assets {
			normalized := normalizeIdentifier(asset.Name)
			object, ok := objectsByName[normalized]
			if !ok {
				parts := strings.Split(normalized, ".")
				if len(parts) > 1 {
					object, ok = objectsByName[parts[len(parts)-1]]
				}
			}

			key := materializationAssetKey(asset.Name, connName)
			item := pipelineMaterializationInfo{
				AssetName:       asset.Name,
				Connection:      connName,
				DeclaredMatType: string(asset.Materialization.Type),
			}

			if ok {
				item.IsMaterialized = true
				item.MaterializedAs = object.Kind

				if count, hasCount := rowCounts[normalizeIdentifier(object.QualifiedName)]; hasCount {
					c := count
					item.RowCount = &c
				} else if count, hasCount := rowCounts[normalizeIdentifier(object.Name)]; hasCount {
					c := count
					item.RowCount = &c
				}
			}

			result[key] = item
		}
	}

	return result
}

func computePipelineFreshness(
	parsed *pipeline.Pipeline,
	matInfo map[string]pipelineMaterializationInfo,
	tracker map[string]freshness.AssetTimestamps,
) map[string]string {
	result := make(map[string]string, len(parsed.Assets))
	assetsByName := make(map[string]*pipeline.Asset, len(parsed.Assets))
	for _, asset := range parsed.Assets {
		assetsByName[asset.Name] = asset
	}

	type visitState int
	const (
		visitUnknown visitState = iota
		visitActive
		visitDone
	)

	type freshnessEval struct {
		Fresh           bool
		EffectiveUpdate *time.Time
	}

	state := make(map[string]visitState, len(parsed.Assets))
	evals := make(map[string]freshnessEval, len(parsed.Assets))

	var evalAsset func(assetName string) freshnessEval
	evalAsset = func(assetName string) freshnessEval {
		if state[assetName] == visitDone {
			return evals[assetName]
		}
		if state[assetName] == visitActive {
			return freshnessEval{Fresh: false}
		}

		asset, ok := assetsByName[assetName]
		if !ok {
			return freshnessEval{Fresh: false}
		}

		state[assetName] = visitActive
		defer func() {
			state[assetName] = visitDone
		}()

		kind := "table"
		connectionName := ""
		if conn, err := parsed.GetConnectionNameForAsset(asset); err == nil {
			connectionName = conn
		}
		if info, ok := matInfo[materializationAssetKey(asset.Name, connectionName)]; ok {
			if strings.EqualFold(strings.TrimSpace(info.MaterializedAs), "view") {
				kind = "view"
			}
		}
		if strings.EqualFold(strings.TrimSpace(string(asset.Materialization.Type)), "view") {
			kind = "view"
		}

		trackerEntry, hasTracker := tracker[assetName]
		var materializedAt *time.Time
		if hasTracker && trackerEntry.MaterializedAt != nil {
			ts := trackerEntry.MaterializedAt.UTC()
			materializedAt = &ts
		}

		upstreamEvals := make([]freshnessEval, 0, len(asset.Upstreams))
		for _, up := range asset.Upstreams {
			upstreamEvals = append(upstreamEvals, evalAsset(up.Value))
		}

		if kind == "view" {
			if len(upstreamEvals) == 0 {
				fresh := materializedAt != nil
				e := freshnessEval{Fresh: fresh, EffectiveUpdate: materializedAt}
				evals[assetName] = e
				return e
			}

			fresh := true
			var latest *time.Time
			for _, up := range upstreamEvals {
				if !up.Fresh {
					fresh = false
				}
				latest = maxTimePtr(latest, up.EffectiveUpdate)
			}

			e := freshnessEval{Fresh: fresh, EffectiveUpdate: latest}
			evals[assetName] = e
			return e
		}

		if materializedAt == nil {
			e := freshnessEval{Fresh: false, EffectiveUpdate: nil}
			evals[assetName] = e
			return e
		}

		fresh := true
		for _, up := range upstreamEvals {
			if !up.Fresh {
				fresh = false
				continue
			}
			if up.EffectiveUpdate != nil && up.EffectiveUpdate.After(*materializedAt) {
				fresh = false
			}
		}

		e := freshnessEval{Fresh: fresh, EffectiveUpdate: materializedAt}
		evals[assetName] = e
		return e
	}

	for _, asset := range parsed.Assets {
		e := evalAsset(asset.Name)
		if e.Fresh {
			result[asset.Name] = "fresh"
		} else {
			result[asset.Name] = "stale"
		}
	}

	return result
}

func maxTimePtr(a, b *time.Time) *time.Time {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if b.After(*a) {
		return b
	}
	return a
}

type dbObjectInfo struct {
	Schema        string
	Name          string
	QualifiedName string
	Kind          string
}

func (s *webServer) fetchObjectsForConnection(ctx context.Context, connectionName string) ([]dbObjectInfo, error) {
	queries := []string{
		`SELECT table_schema, table_name, table_type FROM information_schema.tables`,
		`SHOW TABLES`,
	}

	var rows []map[string]any
	var lastErr error
	for _, query := range queries {
		_, qRows, err := s.runConnectionQuery(ctx, connectionName, query)
		if err != nil {
			lastErr = err
			continue
		}
		rows = qRows
		break
	}

	if len(rows) == 0 {
		return []dbObjectInfo{}, lastErr
	}

	objects := make([]dbObjectInfo, 0, len(rows))
	for _, row := range rows {
		name := readStringField(row, "table_name", "name", "table")
		if name == "" {
			continue
		}

		schema := readStringField(row, "table_schema", "schema", "database")
		qualifiedName := name
		if schema != "" {
			qualifiedName = schema + "." + name
		}

		kind := strings.ToLower(readStringField(row, "table_type", "type"))
		if strings.Contains(kind, "view") {
			kind = "view"
		} else if kind != "" {
			kind = "table"
		} else {
			kind = "table"
		}

		objects = append(objects, dbObjectInfo{
			Schema:        schema,
			Name:          name,
			QualifiedName: qualifiedName,
			Kind:          kind,
		})
	}

	return objects, nil
}

func (s *webServer) fetchRowCountsForObjects(ctx context.Context, connectionName string, objects []dbObjectInfo) map[string]int64 {
	result := make(map[string]int64)
	if len(objects) == 0 {
		return result
	}

	queries := make([]string, 0, len(objects))
	for _, object := range objects {
		queries = append(queries, fmt.Sprintf(
			"SELECT '%s' AS object_name, COUNT(*) AS row_count FROM %s",
			escapeSQLLiteral(object.QualifiedName),
			quoteQualifiedIdentifier(object.QualifiedName),
		))
	}

	countQuery := strings.Join(queries, " UNION ALL ")
	_, rows, err := s.runConnectionQuery(ctx, connectionName, countQuery)
	if err != nil {
		return result
	}

	for _, row := range rows {
		objName := readStringField(row, "object_name")
		if objName == "" {
			continue
		}

		if count, ok := readInt64Field(row, "row_count"); ok {
			result[normalizeIdentifier(objName)] = count
			parts := strings.Split(normalizeIdentifier(objName), ".")
			if len(parts) > 1 {
				result[parts[len(parts)-1]] = count
			}
		}
	}

	return result
}

func (s *webServer) runConnectionQuery(ctx context.Context, connectionName, query string) ([]string, []map[string]any, error) {
	cmdArgs := []string{"query", "--connection", connectionName, "--query", query, "--output", "json"}
	output, err := s.runner.Run(ctx, cmdArgs)
	if err != nil {
		return nil, nil, fmt.Errorf("query failed for connection '%s': %w", connectionName, err)
	}

	columns, rows := parseQueryJSONOutput(output)
	return columns, rows, nil
}

func materializationAssetKey(assetName, connectionName string) string {
	return normalizeIdentifier(assetName) + "|" + normalizeIdentifier(connectionName)
}

func normalizeIdentifier(value string) string {
	replacer := strings.NewReplacer("`", "", `"`, "", "[", "", "]", "")
	clean := replacer.Replace(strings.TrimSpace(value))
	return strings.ToLower(clean)
}

func quoteQualifiedIdentifier(value string) string {
	parts := strings.Split(value, ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(strings.Trim(part, "`\"[]"))
		quoted = append(quoted, `"`+strings.ReplaceAll(trimmed, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, ".")
}

func escapeSQLLiteral(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func readStringField(row map[string]any, keys ...string) string {
	for _, key := range keys {
		for rowKey, value := range row {
			if strings.EqualFold(rowKey, key) {
				s, ok := value.(string)
				if ok {
					return s
				}
			}
		}
	}
	return ""
}

func readInt64Field(row map[string]any, key string) (int64, bool) {
	for rowKey, value := range row {
		if !strings.EqualFold(rowKey, key) {
			continue
		}

		switch v := value.(type) {
		case int:
			return int64(v), true
		case int64:
			return v, true
		case float64:
			return int64(v), true
		case string:
			trimmed := strings.TrimSpace(v)
			if trimmed == "" {
				return 0, false
			}
			var parsed int64
			_, err := fmt.Sscan(trimmed, &parsed)
			if err == nil {
				return parsed, true
			}
		}
	}

	return 0, false
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
	var envelope map[string]any
	if err := json.Unmarshal(output, &envelope); err != nil {
		return []webColumn{}
	}

	rawColumns, ok := envelope["columns"].([]any)
	if !ok {
		return []webColumn{}
	}

	result := make([]webColumn, 0, len(rawColumns))
	for _, raw := range rawColumns {
		if name, ok := raw.(string); ok {
			result = append(result, webColumn{Name: name})
			continue
		}

		mapped, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		name := readStringField(mapped, "name")
		if name == "" {
			continue
		}

		result = append(result, webColumn{
			Name: name,
			Type: readStringField(mapped, "type"),
		})
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
