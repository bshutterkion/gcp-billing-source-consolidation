package bigquery

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
)

// Consolidator builds and executes consolidation queries.
type Consolidator struct {
	Client    *bigquery.Client
	ProjectID string
	DatasetID string
	TableName string
}

// SchemaField represents a column (possibly nested) in a BQ table.
type SchemaField struct {
	Name   string        `json:"name"`
	Type   string        `json:"type"`
	Mode   string        `json:"mode"`
	Fields []SchemaField `json:"fields,omitempty"`
}

// TableSchema holds the full schema for a source table.
type TableSchema struct {
	Source SourceRecord
	Fields []SchemaField
}

// DiscoverSchema fetches the full schema (including nested fields) for a source table.
func (c *Consolidator) DiscoverSchema(ctx context.Context, source SourceRecord) (*TableSchema, error) {
	// Use bq client to get table metadata which includes the full nested schema
	sql := fmt.Sprintf(
		"SELECT column_name, data_type, is_nullable FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '%s' ORDER BY ordinal_position",
		source.SourceProject, source.SourceDataset, source.SourceTable,
	)

	// Instead, use the BQ API to get the schema directly
	factory := &ClientFactory{SAKeyFile: ""}
	srcClient, err := factory.NewClient(ctx, source.SourceProject)
	if err != nil {
		// Fall back to using the consolidator's client for cross-project access
		srcClient = c.Client
	} else {
		defer srcClient.Close()
	}

	// Try using INFORMATION_SCHEMA for column info including struct fields
	_ = sql // not used, we'll use the table metadata API instead

	tbl := srcClient.DatasetInProject(source.SourceProject, source.SourceDataset).Table(source.SourceTable)
	meta, err := tbl.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting schema for %s: %w", BuildSourcesFullRef(source), err)
	}

	fields := bqSchemaToFields(meta.Schema)
	return &TableSchema{Source: source, Fields: fields}, nil
}

func bqSchemaToFields(schema bigquery.Schema) []SchemaField {
	var fields []SchemaField
	for _, f := range schema {
		mode := "NULLABLE"
		if f.Required {
			mode = "REQUIRED"
		}
		if f.Repeated {
			mode = "REPEATED"
		}
		sf := SchemaField{
			Name: f.Name,
			Type: string(f.Type),
			Mode: mode,
		}
		if f.Type == bigquery.RecordFieldType {
			sf.Fields = bqSchemaToFields(f.Schema)
		}
		fields = append(fields, sf)
	}
	return fields
}

// MergedSchema represents the superset schema across all sources.
type MergedSchema struct {
	Fields []MergedField
}

// MergedField is a column in the merged schema, tracking which sources have it.
type MergedField struct {
	Name      string
	Type      string
	Mode      string
	SubFields []MergedField // for RECORD types
}

// MergeSchemas computes the superset schema across all source tables.
func MergeSchemas(schemas []*TableSchema) *MergedSchema {
	merged := &MergedSchema{}
	merged.Fields = mergeFieldLists(schemas, func(ts *TableSchema) []SchemaField { return ts.Fields })
	return merged
}

func mergeFieldLists(schemas []*TableSchema, getFields func(*TableSchema) []SchemaField) []MergedField {
	// Collect all field names in order of first appearance
	seen := make(map[string]bool)
	var orderedNames []string
	fieldsByName := make(map[string][]SchemaField) // all versions of each field

	for _, ts := range schemas {
		for _, f := range getFields(ts) {
			name := f.Name
			if !seen[name] {
				seen[name] = true
				orderedNames = append(orderedNames, name)
			}
			fieldsByName[name] = append(fieldsByName[name], f)
		}
	}

	var merged []MergedField
	for _, name := range orderedNames {
		versions := fieldsByName[name]
		mf := MergedField{
			Name: name,
			Type: versions[0].Type,
			Mode: versions[0].Mode,
		}

		// If this is a RECORD type, merge sub-fields recursively
		if versions[0].Type == "RECORD" {
			// Create pseudo-TableSchemas for sub-fields
			var subSchemas []*TableSchema
			for _, v := range versions {
				subSchemas = append(subSchemas, &TableSchema{Fields: v.Fields})
			}
			mf.SubFields = mergeFieldLists(subSchemas, func(ts *TableSchema) []SchemaField { return ts.Fields })
		}

		merged = append(merged, mf)
	}
	return merged
}

// BuildSelectExpr generates the SELECT expression for a source table against the merged schema.
func BuildSelectExpr(merged *MergedSchema, sourceSchema *TableSchema) string {
	sourceFieldMap := buildFieldMap(sourceSchema.Fields)
	var parts []string
	for _, mf := range merged.Fields {
		parts = append(parts, buildFieldExpr(mf, sourceFieldMap, mf.Name))
	}
	return strings.Join(parts, ", ")
}

func buildFieldMap(fields []SchemaField) map[string]SchemaField {
	m := make(map[string]SchemaField)
	for _, f := range fields {
		m[f.Name] = f
	}
	return m
}

func buildFieldExpr(merged MergedField, sourceFields map[string]SchemaField, path string) string {
	srcField, exists := sourceFields[merged.Name]
	if !exists {
		// Source doesn't have this field — generate NULL with appropriate type
		return fmt.Sprintf("NULL AS %s", merged.Name)
	}

	if merged.Type == "RECORD" && len(merged.SubFields) > 0 {
		// Check if source sub-fields match merged sub-fields exactly
		srcSubMap := buildFieldMap(srcField.Fields)
		allMatch := true
		if len(merged.SubFields) != len(srcField.Fields) {
			allMatch = false
		} else {
			for _, msf := range merged.SubFields {
				if _, ok := srcSubMap[msf.Name]; !ok {
					allMatch = false
					break
				}
			}
		}

		if allMatch {
			// All sub-fields present — just select the column directly
			return merged.Name
		}

		// Need to reconstruct the struct with missing sub-fields
		var subParts []string
		for _, msf := range merged.SubFields {
			if _, ok := srcSubMap[msf.Name]; ok {
				subParts = append(subParts, fmt.Sprintf("%s.%s", merged.Name, msf.Name))
			} else {
				subParts = append(subParts, fmt.Sprintf("CAST(NULL AS %s) AS %s", bqTypeToDDL(msf.Type), msf.Name))
			}
		}
		return fmt.Sprintf("STRUCT(%s) AS %s", strings.Join(subParts, ", "), merged.Name)
	}

	// Simple field — just select it
	return merged.Name
}

// DiscoverAndMergeSchemas discovers schemas for all sources and merges them.
// Sources that cannot be read (e.g., missing table/dataset) are skipped with a warning.
// Returns the merged schema, per-source schemas, and the filtered list of valid sources.
func (c *Consolidator) DiscoverAndMergeSchemas(ctx context.Context, sources []SourceRecord) (*MergedSchema, []*TableSchema, []SourceRecord, error) {
	var schemas []*TableSchema
	var validSources []SourceRecord
	for _, s := range sources {
		ts, err := c.DiscoverSchema(ctx, s)
		if err != nil {
			fmt.Printf("  WARNING: skipping %s: %v\n", BuildSourcesFullRef(s), err)
			continue
		}
		fmt.Printf("  %s: %d top-level columns\n", BuildSourcesFullRef(s), len(ts.Fields))
		schemas = append(schemas, ts)
		validSources = append(validSources, s)
	}

	if len(validSources) == 0 {
		return nil, nil, nil, fmt.Errorf("no valid sources found (all %d sources were skipped)", len(sources))
	}

	if skipped := len(sources) - len(validSources); skipped > 0 {
		fmt.Printf("Skipped %d sources due to errors, proceeding with %d\n", skipped, len(validSources))
	}

	merged := MergeSchemas(schemas)
	fmt.Printf("Merged schema: %d top-level columns\n", len(merged.Fields))

	return merged, schemas, validSources, nil
}

// bqTypeToDDL maps BigQuery API type names to DDL type names.
func bqTypeToDDL(apiType string) string {
	switch apiType {
	case "FLOAT":
		return "FLOAT64"
	case "INTEGER":
		return "INT64"
	case "BOOLEAN":
		return "BOOL"
	default:
		return apiType
	}
}

// BuildColumnList returns a comma-separated list of column names from the merged schema.
func BuildColumnList(merged *MergedSchema) string {
	var names []string
	for _, f := range merged.Fields {
		names = append(names, f.Name)
	}
	return strings.Join(names, ", ")
}

// BuildSchemaDDL generates the column definitions from a merged schema for use in CREATE TABLE.
func BuildSchemaDDL(merged *MergedSchema) string {
	var parts []string
	for _, f := range merged.Fields {
		parts = append(parts, buildColumnDDL(f, 1))
	}
	return strings.Join(parts, ",\n")
}

func buildColumnDDL(f MergedField, indent int) string {
	prefix := strings.Repeat("  ", indent)
	ddlType := bqTypeToDDL(f.Type)
	if f.Type == "RECORD" && len(f.SubFields) > 0 {
		var subParts []string
		for _, sf := range f.SubFields {
			subParts = append(subParts, buildColumnDDL(sf, indent+1))
		}
		structBody := strings.Join(subParts, ",\n")
		if f.Mode == "REPEATED" {
			return fmt.Sprintf("%s%s ARRAY<STRUCT<\n%s\n%s>>", prefix, f.Name, structBody, prefix)
		}
		return fmt.Sprintf("%s%s STRUCT<\n%s\n%s>", prefix, f.Name, structBody, prefix)
	}
	if f.Mode == "REPEATED" {
		return fmt.Sprintf("%s%s ARRAY<%s>", prefix, f.Name, ddlType)
	}
	return fmt.Sprintf("%s%s %s", prefix, f.Name, ddlType)
}

// BuildFullLoadSQL generates the DDL + INSERT statements for full consolidation.
// Uses ingestion-time partitioning (PARTITION BY _PARTITIONDATE) to match the
// partitioning scheme of standard GCP billing export tables. This ensures the
// _PARTITIONTIME pseudo-column is available for queries that depend on it.
func BuildFullLoadSQL(destProject, destDataset, destTable string, sources []SourceRecord, merged *MergedSchema, schemas []*TableSchema) (string, error) {
	if len(sources) == 0 {
		return "", fmt.Errorf("no active sources to consolidate")
	}

	var unions []string
	for i, s := range sources {
		ref := BuildSourcesFullRef(s)
		if merged != nil && schemas != nil {
			selectExpr := BuildSelectExpr(merged, schemas[i])
			unions = append(unions, fmt.Sprintf("  SELECT %s FROM %s", selectExpr, ref))
		} else {
			unions = append(unions, fmt.Sprintf("  SELECT * FROM %s", ref))
		}
	}

	destRef := fmt.Sprintf("`%s.%s.%s`", destProject, destDataset, destTable)
	unionSQL := strings.Join(unions, "\n  UNION ALL\n")

	// Use a multi-statement script:
	// 1. Drop existing table (to handle schema changes)
	// 2. Create table with explicit schema and ingestion-time partitioning
	// 3. Insert data
	if merged != nil {
		schemaDDL := BuildSchemaDDL(merged)
		columnList := BuildColumnList(merged)
		sql := fmt.Sprintf(`-- Drop and recreate with ingestion-time partitioning
DROP TABLE IF EXISTS %s;

CREATE TABLE %s (
%s
)
PARTITION BY _PARTITIONDATE
CLUSTER BY billing_account_id;

INSERT INTO %s (%s)
%s;`,
			destRef, destRef, schemaDDL, destRef, columnList, unionSQL)
		return sql, nil
	}

	// Fallback: if no merged schema available, use a two-step approach
	// Create table from a LIMIT 0 query to get schema, then rebuild with correct partitioning
	sql := fmt.Sprintf(`-- Drop and recreate with ingestion-time partitioning
DROP TABLE IF EXISTS %s;

-- Create with schema from first source
CREATE TABLE %s
PARTITION BY _PARTITIONDATE
CLUSTER BY billing_account_id
AS (SELECT * FROM %s WHERE FALSE);

INSERT INTO %s
%s;`,
		destRef, destRef, BuildSourcesFullRef(sources[0]), destRef, unionSQL)
	return sql, nil
}

// BuildIncrementalSQL generates the DELETE + INSERT script for incremental sync.
func BuildIncrementalSQL(destProject, destDataset, destTable string, sources []SourceRecord, days int, merged *MergedSchema, schemas []*TableSchema) (string, error) {
	if len(sources) == 0 {
		return "", fmt.Errorf("no active sources to consolidate")
	}
	if days <= 0 {
		days = 3
	}

	destRef := fmt.Sprintf("`%s.%s.%s`", destProject, destDataset, destTable)

	var unions []string
	for i, s := range sources {
		ref := BuildSourcesFullRef(s)
		if merged != nil && schemas != nil {
			selectExpr := BuildSelectExpr(merged, schemas[i])
			unions = append(unions, fmt.Sprintf("  SELECT %s FROM %s WHERE usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d DAY)",
				selectExpr, ref, days))
		} else {
			unions = append(unions, fmt.Sprintf("  SELECT * FROM %s WHERE usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d DAY)",
				ref, days))
		}
	}

	// Build column list for INSERT (required for ingestion-time partitioned tables)
	var columnList string
	if merged != nil {
		columnList = BuildColumnList(merged)
	}

	var insertClause string
	if columnList != "" {
		insertClause = fmt.Sprintf("INSERT INTO %s (%s)", destRef, columnList)
	} else {
		insertClause = fmt.Sprintf("INSERT INTO %s", destRef)
	}

	sql := fmt.Sprintf(`-- Delete recent data and re-insert from sources
DELETE FROM %s
WHERE usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d DAY);

%s
%s;`,
		destRef, days, insertClause, strings.Join(unions, "\nUNION ALL\n"))

	return sql, nil
}

// RunFullLoad executes a full consolidation.
func (c *Consolidator) RunFullLoad(ctx context.Context, sources []SourceRecord, dryRun bool) error {
	fmt.Println("Discovering source schemas...")
	merged, schemas, validSources, err := c.DiscoverAndMergeSchemas(ctx, sources)
	if err != nil {
		return fmt.Errorf("discovering schemas: %w", err)
	}

	sql, err := BuildFullLoadSQL(c.ProjectID, c.DatasetID, c.TableName, validSources, merged, schemas)
	if err != nil {
		return err
	}

	if dryRun {
		fmt.Println("-- DRY RUN: Full load SQL --")
		fmt.Println(sql)
		return nil
	}

	fmt.Printf("Running full load from %d sources...\n", len(validSources))
	q := c.Client.Query(sql)
	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("starting full load: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for full load: %w", err)
	}
	if status.Err() != nil {
		return fmt.Errorf("full load failed: %w", status.Err())
	}

	fmt.Printf("Full load complete. Job: %s\n", job.ID())
	return nil
}

// RunIncrementalSync executes an incremental sync for the last N days.
func (c *Consolidator) RunIncrementalSync(ctx context.Context, sources []SourceRecord, days int, dryRun bool) error {
	fmt.Println("Discovering source schemas...")
	merged, schemas, validSources, err := c.DiscoverAndMergeSchemas(ctx, sources)
	if err != nil {
		return fmt.Errorf("discovering schemas: %w", err)
	}

	sql, err := BuildIncrementalSQL(c.ProjectID, c.DatasetID, c.TableName, validSources, days, merged, schemas)
	if err != nil {
		return err
	}

	if dryRun {
		fmt.Println("-- DRY RUN: Incremental sync SQL --")
		fmt.Println(sql)
		return nil
	}

	fmt.Printf("Running incremental sync (last %d days) from %d sources...\n", days, len(validSources))
	q := c.Client.Query(sql)
	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("starting incremental sync: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for incremental sync: %w", err)
	}
	if status.Err() != nil {
		return fmt.Errorf("incremental sync failed: %w", status.Err())
	}

	fmt.Printf("Incremental sync complete. Job: %s\n", job.ID())
	return nil
}

