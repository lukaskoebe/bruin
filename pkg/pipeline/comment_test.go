package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommentRowsToTaskParsesMetaEntries(t *testing.T) {
	t.Parallel()

	asset, err := commentRowsToTask([]string{
		"name: analytics.customers",
		"type: duckdb.sql",
		"meta.bruin_web_inferred_upstreams: analytics.orders,analytics.seed",
	})
	require.NoError(t, err)
	require.NotNil(t, asset)

	assert.Equal(t, "analytics.orders,analytics.seed", asset.Meta["bruin_web_inferred_upstreams"])
}
