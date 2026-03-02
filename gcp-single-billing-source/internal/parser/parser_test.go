package parser

import (
	"archive/zip"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

func TestReadMapping(t *testing.T) {
	// Create a temp CSV
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "mapping.csv")

	f, err := os.Create(csvPath)
	require.NoError(t, err)
	w := csv.NewWriter(f)
	_ = w.Write([]string{"billing_account_id", "project_id", "dataset_id"})
	_ = w.Write([]string{"0110FC-7EF805-B1D01E", "proj-a", "billing_export"})
	_ = w.Write([]string{"01C50B-15317E-A3F7AE", "proj-b", "billing_export"})
	w.Flush()
	f.Close()

	mapping, err := ReadMapping(csvPath)
	require.NoError(t, err)
	assert.Len(t, mapping, 2)
	assert.Equal(t, "proj-a", mapping["0110FC-7EF805-B1D01E"].ProjectID)
	assert.Equal(t, "billing_export", mapping["0110FC-7EF805-B1D01E"].DatasetID)
	assert.Equal(t, "proj-b", mapping["01C50B-15317E-A3F7AE"].ProjectID)
}

func TestReadMapping_MissingFile(t *testing.T) {
	_, err := ReadMapping("/nonexistent/file.csv")
	assert.Error(t, err)
}

func TestReadMapping_EmptyCSV(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "empty.csv")
	f, err := os.Create(csvPath)
	require.NoError(t, err)
	w := csv.NewWriter(f)
	_ = w.Write([]string{"billing_account_id", "project_id", "dataset_id"})
	w.Flush()
	f.Close()

	_, err = ReadMapping(csvPath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "at least one data row")
}

func TestExtractTableName(t *testing.T) {
	tests := []struct {
		filename string
		expected string
	}{
		{
			filename: "gcp_billing_export_v1_0110FC_7EF805_B1D01E sample rows (1).xlsx",
			expected: "gcp_billing_export_v1_0110FC_7EF805_B1D01E",
		},
		{
			filename: "gcp_billing_export_v1_01C50B_15317E_A3F7AE.xlsx",
			expected: "gcp_billing_export_v1_01C50B_15317E_A3F7AE",
		},
		{
			filename: "gcp_billing_export_resource_v1_0110FC_7EF805_B1D01E.xlsx",
			expected: "gcp_billing_export_resource_v1_0110FC_7EF805_B1D01E",
		},
		{
			filename: "random_file.xlsx",
			expected: "",
		},
		{
			filename: "not_a_billing_export.txt",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			result := extractTableName(tt.filename)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestColumnsMatch(t *testing.T) {
	t.Run("identical columns", func(t *testing.T) {
		sources := []SourceInfo{
			{Columns: []string{"a", "b", "c"}},
			{Columns: []string{"a", "b", "c"}},
		}
		assert.True(t, ColumnsMatch(sources))
	})

	t.Run("different column count", func(t *testing.T) {
		sources := []SourceInfo{
			{Columns: []string{"a", "b", "c"}},
			{Columns: []string{"a", "b"}},
		}
		assert.False(t, ColumnsMatch(sources))
	})

	t.Run("different column names", func(t *testing.T) {
		sources := []SourceInfo{
			{Columns: []string{"a", "b", "c"}},
			{Columns: []string{"a", "b", "d"}},
		}
		assert.False(t, ColumnsMatch(sources))
	})

	t.Run("single source", func(t *testing.T) {
		sources := []SourceInfo{
			{Columns: []string{"a", "b"}},
		}
		assert.True(t, ColumnsMatch(sources))
	})

	t.Run("empty sources", func(t *testing.T) {
		assert.True(t, ColumnsMatch(nil))
	})
}

func TestBuildFullLoadSQL(t *testing.T) {
	sources := []SourceInfo{
		{ProjectID: "proj-a", DatasetID: "billing", TableName: "gcp_billing_export_v1_0110FC_7EF805_B1D01E"},
		{ProjectID: "proj-b", DatasetID: "billing", TableName: "gcp_billing_export_v1_01C50B_15317E_A3F7AE"},
	}

	sql := BuildFullLoadSQL("dest-proj", "billing_consolidated", "gcp_billing_export_consolidated", sources)

	assert.Contains(t, sql, "CREATE OR REPLACE TABLE `dest-proj.billing_consolidated.gcp_billing_export_consolidated`")
	assert.Contains(t, sql, "PARTITION BY DATE(usage_start_time)")
	assert.Contains(t, sql, "CLUSTER BY billing_account_id")
	assert.Contains(t, sql, "SELECT * FROM `proj-a.billing.gcp_billing_export_v1_0110FC_7EF805_B1D01E`")
	assert.Contains(t, sql, "SELECT * FROM `proj-b.billing.gcp_billing_export_v1_01C50B_15317E_A3F7AE`")
	assert.Contains(t, sql, "UNION ALL")
}

func TestBuildIncrementalSQL(t *testing.T) {
	sources := []SourceInfo{
		{ProjectID: "proj-a", DatasetID: "billing", TableName: "gcp_billing_export_v1_0110FC_7EF805_B1D01E"},
		{ProjectID: "proj-b", DatasetID: "billing", TableName: "gcp_billing_export_v1_01C50B_15317E_A3F7AE"},
	}

	sql := BuildIncrementalSQL("dest-proj", "billing_consolidated", "gcp_billing_export_consolidated", sources, 5)

	assert.Contains(t, sql, "DECLARE cutoff TIMESTAMP DEFAULT TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 DAY)")
	assert.Contains(t, sql, "DELETE FROM `dest-proj.billing_consolidated.gcp_billing_export_consolidated`")
	assert.Contains(t, sql, "WHERE usage_start_time >= cutoff")
	assert.Contains(t, sql, "INSERT INTO `dest-proj.billing_consolidated.gcp_billing_export_consolidated`")
	assert.Contains(t, sql, "SELECT * FROM `proj-a.billing.gcp_billing_export_v1_0110FC_7EF805_B1D01E` WHERE usage_start_time >= cutoff")
	assert.Contains(t, sql, "UNION ALL")
}

func TestBuildIncrementalSQL_DefaultDays(t *testing.T) {
	sources := []SourceInfo{
		{ProjectID: "proj-a", DatasetID: "ds", TableName: "tbl"},
	}
	sql := BuildIncrementalSQL("p", "d", "t", sources, 0)
	assert.Contains(t, sql, "INTERVAL 3 DAY")
}

// createTestZip creates a zip with xlsx files for testing.
func createTestZip(t *testing.T, dir string, files map[string][]string) string {
	t.Helper()
	zipPath := filepath.Join(dir, "test_exports.zip")
	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	defer zf.Close()

	zw := zip.NewWriter(zf)
	defer zw.Close()

	for name, headers := range files {
		// Create xlsx in memory
		xlsx := excelize.NewFile()
		sheet := xlsx.GetSheetName(0)
		for i, h := range headers {
			cell, _ := excelize.CoordinatesToCellName(i+1, 1)
			_ = xlsx.SetCellValue(sheet, cell, h)
		}
		// Add a data row too
		for i := range headers {
			cell, _ := excelize.CoordinatesToCellName(i+1, 2)
			_ = xlsx.SetCellValue(sheet, cell, "sample_data")
		}

		// Write xlsx to zip
		fw, err := zw.Create(name)
		require.NoError(t, err)
		_, err = xlsx.WriteTo(fw)
		require.NoError(t, err)
	}

	return zipPath
}

func TestParseZip(t *testing.T) {
	dir := t.TempDir()

	headers := []string{"billing_account_id", "project.id", "usage_start_time", "cost"}
	files := map[string][]string{
		"gcp_billing_export_v1_0110FC_7EF805_B1D01E sample.xlsx": headers,
		"gcp_billing_export_v1_01C50B_15317E_A3F7AE.xlsx":        headers,
	}

	zipPath := createTestZip(t, dir, files)

	mapping := map[string]MappingEntry{
		"0110FC-7EF805-B1D01E": {ProjectID: "proj-a", DatasetID: "billing_export"},
		"01C50B-15317E-A3F7AE": {ProjectID: "proj-b", DatasetID: "billing_export"},
	}

	sources, err := ParseZip(zipPath, mapping)
	require.NoError(t, err)
	assert.Len(t, sources, 2)

	// Verify sources were parsed correctly (order may vary due to zip iteration)
	found := map[string]bool{}
	for _, s := range sources {
		found[s.TableName] = true
		assert.Equal(t, headers, s.Columns)
	}
	assert.True(t, found["gcp_billing_export_v1_0110FC_7EF805_B1D01E"])
	assert.True(t, found["gcp_billing_export_v1_01C50B_15317E_A3F7AE"])
}

func TestParseZip_MissingMapping(t *testing.T) {
	dir := t.TempDir()

	files := map[string][]string{
		"gcp_billing_export_v1_0110FC_7EF805_B1D01E.xlsx": {"col1"},
	}
	zipPath := createTestZip(t, dir, files)

	// Empty mapping — should fail
	mapping := map[string]MappingEntry{}

	_, err := ParseZip(zipPath, mapping)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found in mapping")
}

func TestParseZip_SkipsNonXlsx(t *testing.T) {
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "test.zip")

	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	zw := zip.NewWriter(zf)

	// Add a non-xlsx file
	fw, err := zw.Create("readme.txt")
	require.NoError(t, err)
	_, _ = fw.Write([]byte("not an xlsx file"))

	// Add a valid xlsx
	xlsx := excelize.NewFile()
	sheet := xlsx.GetSheetName(0)
	_ = xlsx.SetCellValue(sheet, "A1", "col1")
	fw2, err := zw.Create("gcp_billing_export_v1_0110FC_7EF805_B1D01E.xlsx")
	require.NoError(t, err)
	_, _ = xlsx.WriteTo(fw2)

	zw.Close()
	zf.Close()

	mapping := map[string]MappingEntry{
		"0110FC-7EF805-B1D01E": {ProjectID: "proj-a", DatasetID: "ds"},
	}

	sources, err := ParseZip(zipPath, mapping)
	require.NoError(t, err)
	assert.Len(t, sources, 1)
}

func TestParseZip_DifferentColumns(t *testing.T) {
	dir := t.TempDir()

	files := map[string][]string{
		"gcp_billing_export_v1_0110FC_7EF805_B1D01E.xlsx": {"col1", "col2", "col3"},
		"gcp_billing_export_v1_01C50B_15317E_A3F7AE.xlsx": {"col1", "col2", "col3", "col4"},
	}
	zipPath := createTestZip(t, dir, files)

	mapping := map[string]MappingEntry{
		"0110FC-7EF805-B1D01E": {ProjectID: "proj-a", DatasetID: "ds"},
		"01C50B-15317E-A3F7AE": {ProjectID: "proj-b", DatasetID: "ds"},
	}

	sources, err := ParseZip(zipPath, mapping)
	require.NoError(t, err)
	assert.Len(t, sources, 2)
	assert.False(t, ColumnsMatch(sources))
}
