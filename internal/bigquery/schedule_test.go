package bigquery

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildScheduledQuerySQL_SelectStar(t *testing.T) {
	sources := testSources()
	sql, err := BuildScheduledQuerySQL("dest-proj", "consolidated", "tbl", sources, 3, nil, nil)
	require.NoError(t, err)

	assert.Contains(t, sql, "DECLARE cutoff TIMESTAMP")
	assert.Contains(t, sql, "INTERVAL 3 DAY")
	assert.Contains(t, sql, "DELETE FROM")
	assert.Contains(t, sql, "INSERT INTO")
	assert.Contains(t, sql, "SELECT * FROM")
	assert.Equal(t, 1, strings.Count(sql, "UNION ALL"))
}

func TestBuildScheduledQuerySQL_NoSources(t *testing.T) {
	_, err := BuildScheduledQuerySQL("dest", "ds", "tbl", nil, 3, nil, nil)
	assert.EqualError(t, err, "no active sources for scheduled query")
}

func TestBuildScheduledQuerySQL_DefaultDays(t *testing.T) {
	sql, err := BuildScheduledQuerySQL("dest", "ds", "tbl", testSources(), 0, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, sql, "INTERVAL 3 DAY")
}

func TestBuildScheduledQuerySQL_CustomDays(t *testing.T) {
	sql, err := BuildScheduledQuerySQL("dest", "ds", "tbl", testSources(), 7, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, sql, "INTERVAL 7 DAY")
}

func TestBuildScheduledQuerySQL_WithMergedSchema(t *testing.T) {
	sources := testSources()
	merged := &MergedSchema{
		Fields: []MergedField{
			{Name: "billing_account_id", Type: "STRING", Mode: "NULLABLE"},
			{Name: "cost", Type: "FLOAT64", Mode: "NULLABLE"},
		},
	}
	schemas := []*TableSchema{
		{Fields: []SchemaField{{Name: "billing_account_id", Type: "STRING"}, {Name: "cost", Type: "FLOAT64"}}},
		{Fields: []SchemaField{{Name: "billing_account_id", Type: "STRING"}, {Name: "cost", Type: "FLOAT64"}}},
	}
	sql, err := BuildScheduledQuerySQL("dest", "ds", "tbl", sources, 3, merged, schemas)
	require.NoError(t, err)
	assert.Contains(t, sql, "INSERT INTO `dest.ds.tbl` (billing_account_id, cost)")
	assert.NotContains(t, sql, "SELECT *")
}

func TestBuildScheduledSQL(t *testing.T) {
	sql := BuildScheduledSQL("dest", "ds", "tbl", "meta", 5)
	assert.Contains(t, sql, "INTERVAL 5 DAY")
	assert.Contains(t, sql, "DELETE FROM")
}
