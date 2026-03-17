package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildInferAssetColumnsCommand(t *testing.T) {
	t.Parallel()

	pl := &pipeline.Pipeline{
		DefaultConnections: pipeline.EmptyStringMap{
			"postgres": "warehouse-postgres",
		},
	}
	asset := &pipeline.Asset{
		Name: "analytics.orders",
		Type: pipeline.AssetTypePostgresQuery,
	}

	cmdArgs, err := buildInferAssetColumnsCommand(pl, asset)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"query",
		"--connection",
		"warehouse-postgres",
		"--query",
		`select * from "analytics"."orders" limit 1`,
		"--output",
		"json",
	}, cmdArgs)
}

func TestBuildInferAssetColumnsCommand_RequiresAssetName(t *testing.T) {
	t.Parallel()

	pl := &pipeline.Pipeline{
		DefaultConnections: pipeline.EmptyStringMap{
			"postgres": "warehouse-postgres",
		},
	}
	asset := &pipeline.Asset{Type: pipeline.AssetTypePostgresQuery}

	_, err := buildInferAssetColumnsCommand(pl, asset)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "asset name is required")
}

func TestDefaultAssetContent_PythonTemplate(t *testing.T) {
	t.Parallel()

	content := defaultAssetContent("myschema.my_asset", "python", "assets/my_asset.py")

	require.Contains(t, content, `""" @bruin`)
	require.Contains(t, content, "name: myschema.my_asset")
	require.Contains(t, content, "def materialize():")
	require.Contains(t, content, "import pandas as pd")
}

func TestPythonBruinHeaderRoundTrip_TripleQuote(t *testing.T) {
	t.Parallel()

	current := defaultAssetContent("myschema.my_asset", "python", "assets/my_asset.py")

	executable := extractExecutableContent(current)
	require.Contains(t, executable, "def materialize():")

	updatedExecutable := strings.TrimSpace(`
import pandas as pd

def materialize():
    return pd.DataFrame({"col1": [1]})
`)

	merged := mergeExecutableContent(current, updatedExecutable)

	require.Contains(t, merged, `""" @bruin`)
	require.Contains(t, merged, "name: myschema.my_asset")
	require.Contains(t, merged, `return pd.DataFrame({"col1": [1]})`)
}

func TestEnsurePythonRequirementsFile_CreatesWhenMissing(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	assetPath := filepath.Join(tmp, "assets", "my_python_asset.py")
	require.NoError(t, os.MkdirAll(filepath.Dir(assetPath), 0o755))

	err := ensurePythonRequirementsFile(assetPath, "python", "assets/my_python_asset.py")
	require.NoError(t, err)

	_, statErr := os.Stat(filepath.Join(filepath.Dir(assetPath), "requirements.txt"))
	require.NoError(t, statErr)
}

func TestEnsurePythonRequirementsFile_DoesNotOverwriteExisting(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	assetPath := filepath.Join(tmp, "assets", "my_python_asset.py")
	require.NoError(t, os.MkdirAll(filepath.Dir(assetPath), 0o755))

	requirementsPath := filepath.Join(filepath.Dir(assetPath), "requirements.txt")
	require.NoError(t, os.WriteFile(requirementsPath, []byte("pandas==2.2.2\n"), 0o644))

	err := ensurePythonRequirementsFile(assetPath, "python", "assets/my_python_asset.py")
	require.NoError(t, err)

	content, readErr := os.ReadFile(requirementsPath)
	require.NoError(t, readErr)
	require.Equal(t, "pandas==2.2.2\n", string(content))
}

func TestEnsurePythonRequirementsFile_SkipsNonPython(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	assetPath := filepath.Join(tmp, "assets", "my_sql_asset.sql")
	require.NoError(t, os.MkdirAll(filepath.Dir(assetPath), 0o755))

	err := ensurePythonRequirementsFile(assetPath, "duckdb.sql", "assets/my_sql_asset.sql")
	require.NoError(t, err)

	_, statErr := os.Stat(filepath.Join(filepath.Dir(assetPath), "requirements.txt"))
	require.Error(t, statErr)
	require.True(t, os.IsNotExist(statErr))
}

func TestBuildDownstreamIndex(t *testing.T) {
	t.Parallel()

	// a -> b -> c
	assets := []assetEntry{
		{id: "id_a", name: "a", path: "assets/a.sql"},
		{id: "id_b", name: "b", path: "assets/b.sql", upstreams: []string{"a"}},
		{id: "id_c", name: "c", path: "assets/c.sql", upstreams: []string{"b"}},
	}
	nameToID := map[string]string{"a": "id_a", "b": "id_b", "c": "id_c"}

	ds := buildDownstreamIndex(assets, nameToID)

	assert.Equal(t, []string{"id_b"}, ds["id_a"])
	assert.Equal(t, []string{"id_c"}, ds["id_b"])
	assert.Empty(t, ds["id_c"])
}

func TestPathContains(t *testing.T) {
	t.Parallel()

	tests := []struct {
		eventPath string
		assetPath string
		expected  bool
	}{
		{"assets/a.sql", "assets/a.sql", true},
		{"assets", "assets/a.sql", true},
		{"assets/a.sql", "assets/b.sql", false},
		{"other/x.sql", "assets/a.sql", false},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, pathContains(tt.eventPath, tt.assetPath),
			"pathContains(%q, %q)", tt.eventPath, tt.assetPath)
	}
}

func TestPipelinePathsReferToSameRoot(t *testing.T) {
	t.Parallel()

	root := filepath.Join(string(filepath.Separator), "tmp", "pipeline")

	assert.True(t, pipelinePathsReferToSameRoot(filepath.Join(root, "pipeline.yml"), root))
	assert.True(t, pipelinePathsReferToSameRoot(filepath.Join(root, "pipeline.yaml"), root))
	assert.True(t, pipelinePathsReferToSameRoot(root, filepath.Join(root, "pipeline.yml")))
	assert.False(t, pipelinePathsReferToSameRoot(filepath.Join(root, "pipeline.yml"), filepath.Join(root, "other")))
	assert.False(t, pipelinePathsReferToSameRoot("", root))
}

func TestReplaceAssetNameReferences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		oldName  string
		newName  string
		expected string
	}{
		{
			name:     "replaces fully qualified table reference",
			input:    "select * from marts.old_asset join marts.other on true",
			oldName:  "marts.old_asset",
			newName:  "marts.new_asset",
			expected: "select * from marts.new_asset join marts.other on true",
		},
		{
			name:     "does not replace partial identifier matches",
			input:    "select * from marts.old_asset_backup join marts.old_asset on true",
			oldName:  "marts.old_asset",
			newName:  "marts.new_asset",
			expected: "select * from marts.old_asset_backup join marts.new_asset on true",
		},
		{
			name:     "replaces case insensitive matches",
			input:    "select * from MARTS.OLD_ASSET",
			oldName:  "marts.old_asset",
			newName:  "marts.new_asset",
			expected: "select * from marts.new_asset",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, replaceAssetNameReferences(tt.input, tt.oldName, tt.newName))
		})
	}
}

func TestDeriveDownstreamAssetName(t *testing.T) {
	t.Parallel()

	pl := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{
			{Name: "marts.orders"},
			{Name: "marts.orders_child_1"},
		},
	}

	assert.Equal(t, "marts.orders_child_2", deriveDownstreamAssetName("marts.orders", pl))
}

func TestDefaultDerivedSQLAssetContent(t *testing.T) {
	t.Parallel()

	content := defaultDerivedSQLAssetContent(
		"marts.orders_child_1",
		"duckdb.sql",
		"assets/orders_child_1.sql",
		"marts.orders",
		"duckdb-default",
	)

	require.Contains(t, content, "name: marts.orders_child_1")
	require.Contains(t, content, "type: duckdb.sql")
	require.Contains(t, content, "connection: duckdb-default")
	require.Contains(t, content, "depends:\n  - marts.orders")
	require.Contains(t, content, "select *")
	require.Contains(t, content, `from "marts"."orders"`)
}

func TestDeriveSQLAssetTypeForSource_UsesIngestrTypeConnectionMapping(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Type: pipeline.AssetTypeIngestr,
		Parameters: pipeline.EmptyStringMap{
			"destination": "vertica",
		},
	}

	assert.Equal(t, string(pipeline.AssetTypeVerticaQuery), deriveSQLAssetTypeForSource(asset, nil, ""))
}

func TestDeriveSQLAssetTypeForSource_UsesCanonicalConnectionMappings(t *testing.T) {
	t.Parallel()

	pl := &pipeline.Pipeline{
		DefaultConnections: pipeline.EmptyStringMap{
			"vertica": "warehouse-vertica",
		},
	}
	asset := &pipeline.Asset{Type: pipeline.AssetTypePython}

	assert.Equal(t, string(pipeline.AssetTypeVerticaQuery), deriveSQLAssetTypeForSource(asset, pl, "warehouse-vertica"))
	assert.Equal(t, string(pipeline.AssetTypeVerticaQuery), deriveSQLAssetTypeForSource(asset, pl, "vertica-default"))
}
