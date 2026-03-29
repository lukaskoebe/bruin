package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/spf13/afero"
)

type AssetAPIError struct {
	Status  int
	Code    string
	Message string
}

type AssetUpdateRequest struct {
	Name                *string
	Type                *string
	Content             *string
	MaterializationType *string
	Meta                map[string]string
}

type FormatSQLAssetRequest struct {
	Content string
}

type FormatSQLAssetResponse struct {
	Status  string
	AssetID string
	Content string
	Error   string
}

type AssetDependencies struct {
	WorkspaceRoot                              string
	Runner                                     Runner
	ResolveAssetByID                           func(context.Context, string) (string, *pipeline.Pipeline, *pipeline.Asset, error)
	DefaultAssetContent                        func(string, string, string) string
	DerivedAssetContent                        func(string, string, string, string, string) string
	EnsurePythonRequirements                   func(string, string, string) error
	SuppressWatcher                            func(string)
	PushWorkspaceUpdate                        func(context.Context, string, string)
	PushWorkspaceUpdateImmediate               func(context.Context, string, string)
	PushWorkspaceUpdateImmediateWithChangedIDs func(context.Context, string, string, []string)
}

type AssetService struct {
	deps        AssetDependencies
	patchMu     sync.Mutex
	patchTimers map[string]*time.Timer
}

func NewAssetService(deps AssetDependencies) *AssetService {
	return &AssetService{deps: deps, patchTimers: make(map[string]*time.Timer)}
}

func (s *AssetService) Create(ctx context.Context, pipelineID string, req CreateAssetParams) (map[string]string, *AssetAPIError) {
	relPipelinePath, err := DecodeID(pipelineID)
	if err != nil {
		return nil, &AssetAPIError{Status: 400, Code: "invalid_pipeline_id", Message: "invalid pipeline id"}
	}
	if req.Name == "" && req.Path == "" && req.SourceAssetID == "" {
		return nil, &AssetAPIError{Status: 400, Code: "missing_name_or_path", Message: "name or path is required"}
	}

	pipelinePath, err := SafeJoin(s.deps.WorkspaceRoot, relPipelinePath)
	if err != nil {
		return nil, &AssetAPIError{Status: 400, Code: "invalid_pipeline_path", Message: err.Error()}
	}

	var sourceAsset *pipeline.Asset
	var sourcePipeline *pipeline.Pipeline
	var sourceConnectionName string
	var sourceRelAssetPath string
	if strings.TrimSpace(req.SourceAssetID) != "" {
		resolvedRelPath, resolvedPipeline, resolvedAsset, resolveErr := s.deps.ResolveAssetByID(ctx, req.SourceAssetID)
		if resolveErr != nil {
			return nil, &AssetAPIError{Status: 400, Code: "invalid_source_asset_id", Message: resolveErr.Error()}
		}
		if !pipelinePathsReferToSameRoot(resolvedPipeline.DefinitionFile.Path, pipelinePath) {
			return nil, &AssetAPIError{Status: 400, Code: "invalid_source_asset", Message: "source asset must belong to the selected pipeline"}
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
			sourceAbsAssetPath, pathErr := SafeJoin(s.deps.WorkspaceRoot, sourceRelAssetPath)
			if pathErr != nil {
				return nil, &AssetAPIError{Status: 400, Code: "invalid_source_asset_path", Message: pathErr.Error()}
			}
			sourcePipelineRelativeDir, relErr := filepath.Rel(pipelinePath, filepath.Dir(sourceAbsAssetPath))
			if relErr != nil {
				sourcePipelineRelativeDir = "assets"
			}
			assetTypeForPath := strings.TrimSpace(req.Type)
			if assetTypeForPath == "" {
				assetTypeForPath = deriveSQLAssetTypeForSource(sourceAsset, sourcePipeline, sourceConnectionName)
			}
			relAssetPath = filepath.ToSlash(filepath.Join(sourcePipelineRelativeDir, SlugUnderscore(assetName)+extensionForAssetType(assetTypeForPath)))
		} else {
			relAssetPath = filepath.ToSlash(filepath.Join("assets", SlugUnderscore(assetName)+extensionForAssetType(req.Type)))
		}
	}

	absAssetPath, err := SafeJoin(pipelinePath, relAssetPath)
	if err != nil {
		return nil, &AssetAPIError{Status: 400, Code: "invalid_asset_path", Message: err.Error()}
	}
	if err := os.MkdirAll(filepath.Dir(absAssetPath), 0o755); err != nil {
		return nil, &AssetAPIError{Status: 500, Code: "asset_dir_create_failed", Message: err.Error()}
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
			content = s.deps.DerivedAssetContent(assetName, assetType, relAssetPath, sourceAsset.Name, sourceConnectionName)
		} else {
			content = s.deps.DefaultAssetContent(assetName, assetType, relAssetPath)
		}
	}

	if err := os.WriteFile(absAssetPath, []byte(content), 0o644); err != nil {
		return nil, &AssetAPIError{Status: 500, Code: "asset_write_failed", Message: err.Error()}
	}
	if err := s.deps.EnsurePythonRequirements(absAssetPath, assetType, relAssetPath); err != nil {
		return nil, &AssetAPIError{Status: 500, Code: "requirements_write_failed", Message: err.Error()}
	}

	relWorkspaceAssetPath, _ := filepath.Rel(s.deps.WorkspaceRoot, absAssetPath)
	assetPath := filepath.ToSlash(relWorkspaceAssetPath)
	s.deps.SuppressWatcher(assetPath)
	s.deps.PushWorkspaceUpdateImmediate(ctx, "asset.created", assetPath)
	return map[string]string{"status": "ok", "asset_id": EncodeID(assetPath), "asset_path": assetPath}, nil
}

type CreateAssetParams struct {
	Name          string
	Type          string
	Path          string
	Content       string
	SourceAssetID string
}

func (s *AssetService) Update(ctx context.Context, assetID string, req AssetUpdateRequest) (map[string]string, *AssetAPIError) {
	relAssetPath, err := DecodeID(assetID)
	if err != nil {
		return nil, &AssetAPIError{Status: 400, Code: "invalid_asset_id", Message: "invalid asset id"}
	}
	absAssetPath, err := SafeJoin(s.deps.WorkspaceRoot, relAssetPath)
	if err != nil {
		return nil, &AssetAPIError{Status: 400, Code: "invalid_asset_path", Message: err.Error()}
	}

	originalBytes, err := os.ReadFile(absAssetPath)
	if err != nil {
		return nil, &AssetAPIError{Status: 500, Code: "asset_read_failed", Message: err.Error()}
	}
	desiredExecutable := ExtractExecutableContent(string(originalBytes))
	if req.Content != nil {
		desiredExecutable = *req.Content
	}

	changedAssetIDs := []string{assetID}
	changedAssetPaths := []string{filepath.ToSlash(relAssetPath)}

	if req.Name != nil || req.Type != nil || req.MaterializationType != nil || req.Meta != nil {
		_, parsedPipeline, asset, resolveErr := s.deps.ResolveAssetByID(ctx, assetID)
		if resolveErr != nil {
			return nil, &AssetAPIError{Status: 400, Code: "asset_resolve_failed", Message: resolveErr.Error()}
		}

		originalAssetName := asset.Name
		renamedAsset := false
		if req.Name != nil {
			nextName := strings.TrimSpace(*req.Name)
			if nextName == "" {
				return nil, &AssetAPIError{Status: 400, Code: "invalid_asset_name", Message: "asset name cannot be empty"}
			}
			if existing := parsedPipeline.GetAssetByNameCaseInsensitive(nextName); existing != nil && existing.DefinitionFile.Path != asset.DefinitionFile.Path {
				return nil, &AssetAPIError{Status: 400, Code: "duplicate_asset_name", Message: fmt.Sprintf("an asset named %q already exists", nextName)}
			}
			if nextName != asset.Name {
				asset.Name = nextName
				renamedAsset = true
			}
		}
		if req.Type != nil {
			nextType := strings.TrimSpace(*req.Type)
			if nextType == "" {
				return nil, &AssetAPIError{Status: 400, Code: "invalid_asset_type", Message: "asset type cannot be empty"}
			}
			asset.Type = pipeline.AssetType(nextType)
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
			return nil, &AssetAPIError{Status: 500, Code: "asset_persist_failed", Message: err.Error()}
		}
		if renamedAsset {
			affectedIDs, affectedPaths, refactorErr := s.RefactorDirectDependencies(ctx, parsedPipeline, originalAssetName, asset.Name)
			if refactorErr != nil {
				return nil, &AssetAPIError{Status: 500, Code: "asset_rename_refactor_failed", Message: refactorErr.Error()}
			}
			changedAssetIDs = appendUniqueStrings(changedAssetIDs, affectedIDs...)
			changedAssetPaths = appendUniqueStrings(changedAssetPaths, affectedPaths...)
		}
	}

	currentBytes, err := os.ReadFile(absAssetPath)
	if err != nil {
		return nil, &AssetAPIError{Status: 500, Code: "asset_read_failed", Message: err.Error()}
	}
	mergedContent := MergeExecutableContent(string(currentBytes), desiredExecutable)
	if err := os.WriteFile(absAssetPath, []byte(mergedContent), 0o644); err != nil {
		return nil, &AssetAPIError{Status: 500, Code: "asset_write_failed", Message: err.Error()}
	}

	if req.Content != nil && strings.HasSuffix(strings.ToLower(relAssetPath), ".sql") {
		s.ScheduleSQLPatches(relAssetPath)
	}
	for _, changedPath := range changedAssetPaths {
		s.deps.SuppressWatcher(changedPath)
	}
	s.deps.PushWorkspaceUpdateImmediateWithChangedIDs(ctx, "asset.updated", relAssetPath, changedAssetIDs)
	return map[string]string{"status": "ok"}, nil
}

func (s *AssetService) RefactorDirectDependencies(ctx context.Context, parsedPipeline *pipeline.Pipeline, oldName, newName string) ([]string, []string, error) {
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
			nextContent := ReplaceAssetNameReferences(current.ExecutableFile.Content, oldName, newName)
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
			if err := updateSQLAssetDependencies(ctx, current, parsedPipeline, sqlParserInstance, renderer); err != nil {
				return nil, nil, fmt.Errorf("failed to refresh dependencies for asset '%s': %w", current.Name, err)
			}
		}

		assetPath := current.ExecutableFile.Path
		if assetPath == "" {
			assetPath = current.DefinitionFile.Path
		}

		relAssetPath, relErr := filepath.Rel(s.deps.WorkspaceRoot, assetPath)
		if relErr != nil {
			relAssetPath = assetPath
		}

		normalizedPath := filepath.ToSlash(relAssetPath)
		changedIDs = append(changedIDs, EncodeID(normalizedPath))
		changedPaths = append(changedPaths, normalizedPath)
	}

	return changedIDs, changedPaths, nil
}

func (s *AssetService) ScheduleSQLPatches(relAssetPath string) {
	assetPath := filepath.ToSlash(relAssetPath)

	s.patchMu.Lock()
	if existing, ok := s.patchTimers[assetPath]; ok {
		existing.Stop()
	}

	s.patchTimers[assetPath] = time.AfterFunc(1500*time.Millisecond, func() {
		s.RunSQLPatches(assetPath)

		s.patchMu.Lock()
		delete(s.patchTimers, assetPath)
		s.patchMu.Unlock()
	})
	s.patchMu.Unlock()
}

func (s *AssetService) RunSQLPatches(relAssetPath string) {
	prefixedPath := relAssetPath
	if !strings.HasPrefix(prefixedPath, ".") {
		prefixedPath = "." + prefixedPath
	}

	commands := [][]string{
		{"patch", "fill-columns-from-db", prefixedPath},
		{"patch", "fill-asset-dependencies", relAssetPath},
	}

	for _, args := range commands {
		_, _ = s.deps.Runner.Run(context.Background(), args)
	}

	s.deps.SuppressWatcher(relAssetPath)
	if s.deps.PushWorkspaceUpdate != nil {
		s.deps.PushWorkspaceUpdate(context.Background(), "asset.patched", relAssetPath)
	}
}

func (s *AssetService) Delete(ctx context.Context, assetID string) (map[string]string, *AssetAPIError) {
	relAssetPath, err := DecodeID(assetID)
	if err != nil {
		return nil, &AssetAPIError{Status: 400, Code: "invalid_asset_id", Message: "invalid asset id"}
	}
	absAssetPath, err := SafeJoin(s.deps.WorkspaceRoot, relAssetPath)
	if err != nil {
		return nil, &AssetAPIError{Status: 400, Code: "invalid_asset_path", Message: err.Error()}
	}
	if err := os.Remove(absAssetPath); err != nil {
		return nil, &AssetAPIError{Status: 500, Code: "asset_delete_failed", Message: err.Error()}
	}
	s.deps.SuppressWatcher(relAssetPath)
	s.deps.PushWorkspaceUpdateImmediate(ctx, "asset.deleted", relAssetPath)
	return map[string]string{"status": "ok"}, nil
}

func (s *AssetService) FormatSQL(ctx context.Context, assetID string, req FormatSQLAssetRequest) (FormatSQLAssetResponse, *AssetAPIError) {
	relAssetPath, err := DecodeID(assetID)
	if err != nil {
		return FormatSQLAssetResponse{}, &AssetAPIError{Status: 400, Code: "invalid_asset_id", Message: "invalid asset id"}
	}
	if !strings.HasSuffix(strings.ToLower(relAssetPath), ".sql") {
		return FormatSQLAssetResponse{}, &AssetAPIError{Status: 400, Code: "invalid_asset_type", Message: "only SQL assets can be formatted"}
	}
	absAssetPath, err := SafeJoin(s.deps.WorkspaceRoot, relAssetPath)
	if err != nil {
		return FormatSQLAssetResponse{}, &AssetAPIError{Status: 400, Code: "invalid_asset_path", Message: err.Error()}
	}
	originalBytes, err := os.ReadFile(absAssetPath)
	if err != nil {
		return FormatSQLAssetResponse{}, &AssetAPIError{Status: 500, Code: "asset_read_failed", Message: err.Error()}
	}
	mergedContent := MergeExecutableContent(string(originalBytes), req.Content)
	if err := os.WriteFile(absAssetPath, []byte(mergedContent), 0o644); err != nil {
		return FormatSQLAssetResponse{}, &AssetAPIError{Status: 500, Code: "asset_write_failed", Message: err.Error()}
	}
	output, err := s.deps.Runner.Run(ctx, []string{"format", relAssetPath, "--sqlfluff"})
	if err != nil {
		return FormatSQLAssetResponse{Status: "error", AssetID: assetID, Content: req.Content, Error: strings.TrimSpace(string(output))}, nil
	}
	formattedBytes, err := os.ReadFile(absAssetPath)
	if err != nil {
		return FormatSQLAssetResponse{}, &AssetAPIError{Status: 500, Code: "asset_read_failed", Message: err.Error()}
	}
	s.deps.SuppressWatcher(relAssetPath)
	s.deps.PushWorkspaceUpdateImmediateWithChangedIDs(ctx, "asset.updated", relAssetPath, []string{assetID})
	return FormatSQLAssetResponse{Status: "ok", AssetID: assetID, Content: ExtractExecutableContent(string(formattedBytes))}, nil
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

func ReplaceAssetNameReferences(content, oldName, newName string) string {
	trimmedOld := strings.TrimSpace(oldName)
	trimmedNew := strings.TrimSpace(newName)
	if trimmedOld == "" || trimmedNew == "" || trimmedOld == trimmedNew {
		return content
	}

	pattern := fmt.Sprintf(`(?i)(^|[^A-Za-z0-9_.])(%s)([^A-Za-z0-9_.]|$)`, regexp.QuoteMeta(trimmedOld))
	re := regexp.MustCompile(pattern)
	return re.ReplaceAllString(content, `${1}`+trimmedNew+`${3}`)
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

func updateSQLAssetDependencies(ctx context.Context, asset *pipeline.Asset, parsedPipeline *pipeline.Pipeline, sqlParserInstance *sqlparser.SQLParser, renderer *jinja.Renderer) error {
	assetRenderer, err := renderer.CloneForAsset(ctx, parsedPipeline, asset)
	if err != nil {
		return fmt.Errorf("failed to create renderer for asset '%s': %w", asset.Name, err)
	}

	missingDeps, err := sqlParserInstance.GetMissingDependenciesForAsset(asset, parsedPipeline, assetRenderer)
	if err != nil {
		return fmt.Errorf("failed to get missing dependencies for asset '%s': %w", asset.Name, err)
	}

	if len(missingDeps) == 0 {
		return nil
	}

	for _, dep := range missingDeps {
		foundMissingUpstream := parsedPipeline.GetAssetByNameCaseInsensitive(dep)
		if foundMissingUpstream == nil {
			continue
		}

		if foundMissingUpstream.Name == asset.Name {
			continue
		}

		asset.AddUpstream(foundMissingUpstream)
	}

	if err := asset.Persist(afero.NewOsFs()); err != nil {
		return fmt.Errorf("failed to persist asset '%s': %w", asset.Name, err)
	}

	return nil
}

func pipelinePathsReferToSameRoot(sourcePipelinePath, targetPipelineRoot string) bool {
	normalizedSource := filepath.Clean(sourcePipelinePath)
	normalizedTarget := filepath.Clean(targetPipelineRoot)
	if normalizedSource == normalizedTarget {
		return true
	}
	return filepath.Clean(filepath.Dir(normalizedSource)) == normalizedTarget
}

func extensionForAssetType(assetType string) string {
	lowered := strings.ToLower(strings.TrimSpace(assetType))
	switch {
	case strings.HasSuffix(lowered, ".py") || strings.Contains(lowered, "python"):
		return ".py"
	case strings.HasSuffix(lowered, ".sql") || strings.Contains(lowered, "sql"):
		return ".sql"
	default:
		return ".sql"
	}
}

func inferAssetTypeFromPath(path string) string {
	lowered := strings.ToLower(strings.TrimSpace(path))
	switch filepath.Ext(lowered) {
	case ".py":
		return "python"
	case ".sql":
		return "duckdb.sql"
	default:
		return "duckdb.sql"
	}
}

func deriveDownstreamAssetName(sourceAssetName string, parsedPipeline *pipeline.Pipeline) string {
	base := sourceAssetName
	if idx := strings.LastIndex(base, "."); idx >= 0 && idx < len(base)-1 {
		base = base[idx+1:]
	}
	base = SlugUnderscore(base)
	if base == "" {
		base = "asset"
	}
	candidate := base + "_downstream"
	if parsedPipeline == nil {
		return candidate
	}
	if parsedPipeline.GetAssetByNameCaseInsensitive(candidate) == nil {
		return candidate
	}
	for index := 2; index < 1000; index += 1 {
		next := fmt.Sprintf("%s_%d", candidate, index)
		if parsedPipeline.GetAssetByNameCaseInsensitive(next) == nil {
			return next
		}
	}
	return candidate
}

func deriveSQLAssetTypeForSource(sourceAsset *pipeline.Asset, parsedPipeline *pipeline.Pipeline, sourceConnectionName string) string {
	if sourceAsset != nil {
		assetType := strings.TrimSpace(string(sourceAsset.Type))
		if strings.Contains(strings.ToLower(assetType), "sql") {
			return assetType
		}
	}
	if strings.TrimSpace(sourceConnectionName) != "" {
		return strings.ToLower(sourceConnectionName) + ".sql"
	}
	if parsedPipeline != nil {
		for _, current := range parsedPipeline.Assets {
			if current == nil {
				continue
			}
			assetType := strings.TrimSpace(string(current.Type))
			if strings.Contains(strings.ToLower(assetType), "sql") {
				return assetType
			}
		}
	}
	return "duckdb.sql"
}

func DefaultDerivedSQLAssetContent(assetName, assetType, assetPath, sourceAssetName, connectionName string) string {
	header := fmt.Sprintf("/* @bruin\n\nname: %s\ntype: %s\nmaterialization:\n  type: view\n\n@bruin */\n\n", assetName, assetType)
	queryTarget := sourceAssetName
	if strings.TrimSpace(queryTarget) == "" {
		queryTarget = strings.TrimSuffix(filepath.Base(assetPath), filepath.Ext(assetPath))
	}
	query := fmt.Sprintf("select * from %s\n", queryTarget)
	if strings.TrimSpace(connectionName) != "" {
		query += fmt.Sprintf("-- source connection: %s\n", connectionName)
	}
	return header + query
}

func EnsurePythonRequirementsFile(absAssetPath, assetType, relAssetPath string) error {
	loweredType := strings.ToLower(strings.TrimSpace(assetType))
	loweredPath := strings.ToLower(strings.TrimSpace(relAssetPath))
	if !strings.HasSuffix(loweredPath, ".py") && !strings.Contains(loweredType, "python") {
		return nil
	}
	requirementsPath := filepath.Join(filepath.Dir(absAssetPath), "requirements.txt")
	if _, err := os.Stat(requirementsPath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.WriteFile(requirementsPath, []byte("pandas\n"), 0o644)
}

func defaultDerivedSQLAssetContent(assetName, assetType, assetPath, sourceAssetName, connectionName string) string {
	return DefaultDerivedSQLAssetContent(assetName, assetType, assetPath, sourceAssetName, connectionName)
}

func ensurePythonRequirementsFile(absAssetPath, assetType, relAssetPath string) error {
	return EnsurePythonRequirementsFile(absAssetPath, assetType, relAssetPath)
}
