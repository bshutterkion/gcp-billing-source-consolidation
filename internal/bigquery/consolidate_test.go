package bigquery

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testSources() []SourceRecord {
	return []SourceRecord{
		{SourceProject: "proj-a", SourceDataset: "billing", SourceTable: "gcp_billing_export_v1_0110FC_7EF805_B1D01E"},
		{SourceProject: "proj-b", SourceDataset: "billing", SourceTable: "gcp_billing_export_v1_01C50B_15317E_A3F7AE"},
	}
}

func TestBuildFullLoadSQL_SelectStar(t *testing.T) {
	sql, err := BuildFullLoadSQL("dest-proj", "consolidated", "tbl", testSources(), nil, nil)
	require.NoError(t, err)

	assert.Contains(t, sql, "DROP TABLE IF EXISTS")
	assert.Contains(t, sql, "CREATE TABLE")
	assert.Contains(t, sql, "PARTITION BY _PARTITIONDATE")
	assert.Contains(t, sql, "INSERT INTO")
	assert.Contains(t, sql, "SELECT * FROM")
	assert.Equal(t, 1, strings.Count(sql, "UNION ALL"))
}

func TestBuildFullLoadSQL_WithMergedSchema(t *testing.T) {
	sources := testSources()
	merged := &MergedSchema{
		Fields: []MergedField{
			{Name: "billing_account_id", Type: "STRING"},
			{Name: "cost", Type: "FLOAT"},
			{Name: "invoice", Type: "RECORD", SubFields: []MergedField{
				{Name: "month", Type: "STRING"},
				{Name: "publisher_type", Type: "STRING"},
			}},
		},
	}
	schemas := []*TableSchema{
		{Source: sources[0], Fields: []SchemaField{
			{Name: "billing_account_id", Type: "STRING"},
			{Name: "cost", Type: "FLOAT"},
			{Name: "invoice", Type: "RECORD", Fields: []SchemaField{
				{Name: "month", Type: "STRING"},
				{Name: "publisher_type", Type: "STRING"},
			}},
		}},
		{Source: sources[1], Fields: []SchemaField{
			{Name: "billing_account_id", Type: "STRING"},
			{Name: "cost", Type: "FLOAT"},
			{Name: "invoice", Type: "RECORD", Fields: []SchemaField{
				{Name: "month", Type: "STRING"},
			}},
		}},
	}

	sql, err := BuildFullLoadSQL("dest", "ds", "tbl", sources, merged, schemas)
	require.NoError(t, err)

	// Source A has full invoice struct
	assert.Contains(t, sql, "billing_account_id, cost, invoice FROM `proj-a")
	// Source B is missing publisher_type in invoice
	assert.Contains(t, sql, "STRUCT(invoice.month, CAST(NULL AS STRING) AS publisher_type) AS invoice FROM `proj-b")
}

func TestBuildFullLoadSQL_NoSources(t *testing.T) {
	_, err := BuildFullLoadSQL("dest", "ds", "tbl", nil, nil, nil)
	assert.EqualError(t, err, "no active sources to consolidate")
}

func TestBuildIncrementalSQL_SelectStar(t *testing.T) {
	sql, err := BuildIncrementalSQL("dest", "ds", "tbl", testSources(), 3, nil, nil)
	require.NoError(t, err)

	assert.Contains(t, sql, "DELETE FROM")
	assert.Contains(t, sql, "INTERVAL 3 DAY")
	assert.Contains(t, sql, "SELECT * FROM")
}

func TestBuildIncrementalSQL_DefaultDays(t *testing.T) {
	sql, err := BuildIncrementalSQL("dest", "ds", "tbl", testSources(), 0, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, sql, "INTERVAL 3 DAY")
}

func TestBuildIncrementalSQL_NoSources(t *testing.T) {
	_, err := BuildIncrementalSQL("dest", "ds", "tbl", nil, 3, nil, nil)
	assert.EqualError(t, err, "no active sources to consolidate")
}

func TestBuildSelectExpr_AllFieldsPresent(t *testing.T) {
	merged := &MergedSchema{
		Fields: []MergedField{
			{Name: "a", Type: "STRING"},
			{Name: "b", Type: "INTEGER"},
		},
	}
	ts := &TableSchema{Fields: []SchemaField{
		{Name: "a", Type: "STRING"},
		{Name: "b", Type: "INTEGER"},
	}}
	expr := BuildSelectExpr(merged, ts)
	assert.Equal(t, "a, b", expr)
}

func TestBuildSelectExpr_MissingField(t *testing.T) {
	merged := &MergedSchema{
		Fields: []MergedField{
			{Name: "a", Type: "STRING"},
			{Name: "b", Type: "INTEGER"},
		},
	}
	ts := &TableSchema{Fields: []SchemaField{
		{Name: "a", Type: "STRING"},
	}}
	expr := BuildSelectExpr(merged, ts)
	assert.Equal(t, "a, NULL AS b", expr)
}

func TestBuildSelectExpr_StructMismatch(t *testing.T) {
	merged := &MergedSchema{
		Fields: []MergedField{
			{Name: "invoice", Type: "RECORD", SubFields: []MergedField{
				{Name: "month", Type: "STRING"},
				{Name: "publisher_type", Type: "STRING"},
			}},
		},
	}
	ts := &TableSchema{Fields: []SchemaField{
		{Name: "invoice", Type: "RECORD", Fields: []SchemaField{
			{Name: "month", Type: "STRING"},
		}},
	}}
	expr := BuildSelectExpr(merged, ts)
	assert.Contains(t, expr, "STRUCT(invoice.month, CAST(NULL AS STRING) AS publisher_type) AS invoice")
}

func TestMergeSchemas(t *testing.T) {
	schemas := []*TableSchema{
		{Fields: []SchemaField{
			{Name: "a", Type: "STRING"},
			{Name: "b", Type: "RECORD", Fields: []SchemaField{
				{Name: "x", Type: "STRING"},
			}},
		}},
		{Fields: []SchemaField{
			{Name: "a", Type: "STRING"},
			{Name: "b", Type: "RECORD", Fields: []SchemaField{
				{Name: "x", Type: "STRING"},
				{Name: "y", Type: "STRING"},
			}},
			{Name: "c", Type: "INTEGER"},
		}},
	}

	merged := MergeSchemas(schemas)
	assert.Len(t, merged.Fields, 3) // a, b, c
	assert.Equal(t, "b", merged.Fields[1].Name)
	assert.Len(t, merged.Fields[1].SubFields, 2) // x, y
	assert.Equal(t, "c", merged.Fields[2].Name)
}
