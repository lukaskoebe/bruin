package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
