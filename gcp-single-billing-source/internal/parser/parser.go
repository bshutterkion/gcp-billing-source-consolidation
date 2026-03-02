package parser

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	bq "github.com/kionsoftware/billing-consolidator/internal/bigquery"
	"github.com/xuri/excelize/v2"
)

// MappingEntry maps a billing account to its project and dataset.
type MappingEntry struct {
	ProjectID string
	DatasetID string
}

// SourceInfo holds all info needed to generate SQL for one billing export.
type SourceInfo struct {
	ProjectID string
	DatasetID string
	TableName string
	Columns   []string
}

// ReadMapping parses a CSV mapping file with columns: billing_account_id,project_id,dataset_id.
func ReadMapping(path string) (map[string]MappingEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening mapping file: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("reading mapping CSV: %w", err)
	}

	if len(records) < 2 {
		return nil, fmt.Errorf("mapping CSV must have a header row and at least one data row")
	}

	// Validate header
	header := records[0]
	if len(header) < 3 {
		return nil, fmt.Errorf("mapping CSV must have columns: billing_account_id,project_id,dataset_id")
	}

	mapping := make(map[string]MappingEntry)
	for _, row := range records[1:] {
		if len(row) < 3 {
			continue
		}
		billingID := strings.TrimSpace(row[0])
		mapping[billingID] = MappingEntry{
			ProjectID: strings.TrimSpace(row[1]),
			DatasetID: strings.TrimSpace(row[2]),
		}
	}

	return mapping, nil
}

// ParseZip opens a zip of xlsx files and extracts source info for each billing export.
// It uses the mapping to look up project/dataset for each billing account.
func ParseZip(zipPath string, mapping map[string]MappingEntry) ([]SourceInfo, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, fmt.Errorf("opening zip: %w", err)
	}
	defer r.Close()

	var sources []SourceInfo

	for _, f := range r.File {
		if f.FileInfo().IsDir() {
			continue
		}

		// Only process xlsx files
		name := filepath.Base(f.Name)
		if !strings.HasSuffix(strings.ToLower(name), ".xlsx") {
			continue
		}

		// Extract table name from filename — strip everything after the billing account hex pattern.
		// Filenames look like: "gcp_billing_export_v1_0110FC_7EF805_B1D01E sample rows (1).xlsx"
		// We need just: "gcp_billing_export_v1_0110FC_7EF805_B1D01E"
		tableName := extractTableName(name)
		if tableName == "" {
			fmt.Printf("  Skipping %s: cannot extract table name\n", name)
			continue
		}

		billingID, _, ok := bq.ExtractBillingAccountID(tableName)
		if !ok {
			fmt.Printf("  Skipping %s: cannot extract billing account ID\n", name)
			continue
		}

		entry, found := mapping[billingID]
		if !found {
			return nil, fmt.Errorf("billing account %s (from %s) not found in mapping CSV", billingID, name)
		}

		// Read xlsx headers
		columns, err := readXlsxHeaders(f)
		if err != nil {
			return nil, fmt.Errorf("reading headers from %s: %w", name, err)
		}

		sources = append(sources, SourceInfo{
			ProjectID: entry.ProjectID,
			DatasetID: entry.DatasetID,
			TableName: tableName,
			Columns:   columns,
		})

		fmt.Printf("  %s: %d columns (billing account: %s)\n", tableName, len(columns), billingID)
	}

	if len(sources) == 0 {
		return nil, fmt.Errorf("no valid xlsx billing exports found in zip")
	}

	return sources, nil
}

// extractTableName extracts the BQ table name from an xlsx filename.
// It finds the billing export prefix pattern and returns just the table name portion.
func extractTableName(filename string) string {
	// Remove .xlsx extension
	name := strings.TrimSuffix(filename, filepath.Ext(filename))

	// Try to match a standard or detailed export pattern at the start.
	// The table name ends after the third hex group (6 chars each separated by underscores).
	// Try standard first: gcp_billing_export_v1_XXXXXX_YYYYYY_ZZZZZZ
	// Then detailed: gcp_billing_export_resource_v1_XXXXXX_YYYYYY_ZZZZZZ
	for _, prefix := range []string{"gcp_billing_export_resource_v1_", "gcp_billing_export_v1_"} {
		idx := strings.Index(strings.ToLower(name), strings.ToLower(prefix))
		if idx < 0 {
			continue
		}
		// From the prefix start, extract prefix + 3 hex groups (6 chars each with 2 underscores)
		start := idx
		rest := name[start+len(prefix):]
		// Expect XXXXXX_YYYYYY_ZZZZZZ (20 chars)
		if len(rest) < 20 {
			continue
		}
		candidate := name[start : start+len(prefix)+20]
		// Validate using ExtractBillingAccountID
		if _, _, ok := bq.ExtractBillingAccountID(candidate); ok {
			return candidate
		}
	}
	return ""
}

// readXlsxHeaders reads the first row of the first sheet in an xlsx file from a zip entry.
func readXlsxHeaders(f *zip.File) ([]string, error) {
	rc, err := f.Open()
	if err != nil {
		return nil, fmt.Errorf("opening zip entry: %w", err)
	}
	defer rc.Close()

	xlsx, err := excelize.OpenReader(rc)
	if err != nil {
		return nil, fmt.Errorf("opening xlsx: %w", err)
	}
	defer xlsx.Close()

	sheet := xlsx.GetSheetName(0)
	if sheet == "" {
		return nil, fmt.Errorf("no sheets found")
	}

	rows, err := xlsx.GetRows(sheet)
	if err != nil {
		return nil, fmt.Errorf("reading rows: %w", err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("xlsx has no rows")
	}

	// Filter out empty column headers
	var columns []string
	for _, col := range rows[0] {
		col = strings.TrimSpace(col)
		if col != "" {
			columns = append(columns, col)
		}
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no column headers found in first row")
	}

	return columns, nil
}

// ColumnsMatch returns true if all sources have identical column sets.
func ColumnsMatch(sources []SourceInfo) bool {
	if len(sources) <= 1 {
		return true
	}
	ref := sources[0].Columns
	for _, s := range sources[1:] {
		if len(s.Columns) != len(ref) {
			return false
		}
		for i, c := range s.Columns {
			if c != ref[i] {
				return false
			}
		}
	}
	return true
}

// BuildFullLoadSQL generates the CREATE OR REPLACE TABLE SQL for offline sources.
func BuildFullLoadSQL(destProject, destDataset, destTable string, sources []SourceInfo) string {
	var unions []string
	for _, s := range sources {
		ref := fmt.Sprintf("`%s.%s.%s`", s.ProjectID, s.DatasetID, s.TableName)
		unions = append(unions, fmt.Sprintf("  SELECT * FROM %s", ref))
	}

	return fmt.Sprintf(`CREATE OR REPLACE TABLE `+"`%s.%s.%s`"+`
PARTITION BY DATE(usage_start_time)
CLUSTER BY billing_account_id
AS (
%s
)`, destProject, destDataset, destTable, strings.Join(unions, "\n  UNION ALL\n"))
}

// BuildIncrementalSQL generates the scheduled query SQL (DECLARE/DELETE/INSERT) for offline sources.
func BuildIncrementalSQL(destProject, destDataset, destTable string, sources []SourceInfo, days int) string {
	if days <= 0 {
		days = 3
	}

	destRef := fmt.Sprintf("`%s.%s.%s`", destProject, destDataset, destTable)

	var unions []string
	for _, s := range sources {
		ref := fmt.Sprintf("`%s.%s.%s`", s.ProjectID, s.DatasetID, s.TableName)
		unions = append(unions, fmt.Sprintf("  SELECT * FROM %s WHERE usage_start_time >= cutoff", ref))
	}

	return fmt.Sprintf(`DECLARE cutoff TIMESTAMP DEFAULT TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d DAY);

DELETE FROM %s
WHERE usage_start_time >= cutoff;

INSERT INTO %s
%s;`,
		days, destRef, destRef, strings.Join(unions, "\nUNION ALL\n"))
}

// PrintColumnWarnings prints warnings about column mismatches between sources.
func PrintColumnWarnings(sources []SourceInfo) {
	if len(sources) <= 1 {
		return
	}

	// Build map of column counts
	type colInfo struct {
		tableName string
		count     int
	}
	var infos []colInfo
	for _, s := range sources {
		infos = append(infos, colInfo{tableName: s.TableName, count: len(s.Columns)})
	}

	// Check if counts differ
	allSame := true
	for _, info := range infos[1:] {
		if info.count != infos[0].count {
			allSame = false
			break
		}
	}
	if allSame {
		return
	}

	fmt.Println("\nWARNING: Column counts differ across sources (xlsx headers):")
	for _, info := range infos {
		fmt.Printf("  %s: %d columns\n", info.tableName, info.count)
	}

	// Show columns unique to each source
	allCols := make(map[string][]string) // col -> list of tables that have it
	for _, s := range sources {
		for _, c := range s.Columns {
			allCols[c] = append(allCols[c], s.TableName)
		}
	}

	var extraCols []string
	for col, tables := range allCols {
		if len(tables) < len(sources) {
			extraCols = append(extraCols, col)
		}
	}
	sort.Strings(extraCols)

	if len(extraCols) > 0 {
		fmt.Println("  Columns not present in all sources:")
		for _, col := range extraCols {
			fmt.Printf("    %s (in: %s)\n", col, strings.Join(allCols[col], ", "))
		}
	}

	fmt.Println("  Note: SQL uses SELECT * since BQ schema (not xlsx headers) determines column resolution.")
	fmt.Println("  If UNION ALL fails at runtime, source schemas may need explicit column alignment.")
}
