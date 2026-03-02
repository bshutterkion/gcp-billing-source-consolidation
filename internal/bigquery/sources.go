package bigquery

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// SourceRecord represents a row in the billing_export_sources metadata table.
type SourceRecord struct {
	SourceProject    string    `bigquery:"source_project"`
	SourceDataset    string    `bigquery:"source_dataset"`
	SourceTable      string    `bigquery:"source_table"`
	BillingAccountID string    `bigquery:"billing_account_id"`
	IsActive         bool      `bigquery:"is_active"`
	AddedAt          time.Time `bigquery:"added_at"`
}

// SourceManager handles CRUD operations on the billing_export_sources metadata table.
type SourceManager struct {
	Client    *bigquery.Client
	ProjectID string
	DatasetID string
}

// CreateMetadataTable creates the billing_export_sources table if it doesn't exist.
func (m *SourceManager) CreateMetadataTable(ctx context.Context) error {
	query := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s.billing_export_sources (
  source_project STRING NOT NULL,
  source_dataset STRING NOT NULL,
  source_table STRING NOT NULL,
  billing_account_id STRING NOT NULL,
  is_active BOOL DEFAULT TRUE,
  added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)`, m.ProjectID, m.DatasetID)

	q := m.Client.Query(query)
	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("creating metadata table: %w", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for metadata table creation: %w", err)
	}
	if status.Err() != nil {
		return fmt.Errorf("metadata table creation failed: %w", status.Err())
	}
	return nil
}

// AddSource inserts a new source into the metadata table if it doesn't already exist.
func (m *SourceManager) AddSource(ctx context.Context, source BillingExportSource) (bool, error) {
	// Check for duplicate
	exists, err := m.sourceExists(ctx, source.ProjectID, source.DatasetID, source.TableName)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}

	query := fmt.Sprintf(`
INSERT INTO %s.%s.billing_export_sources (source_project, source_dataset, source_table, billing_account_id)
VALUES (@project, @dataset, @table_name, @billing_id)`,
		m.ProjectID, m.DatasetID)

	q := m.Client.Query(query)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "project", Value: source.ProjectID},
		{Name: "dataset", Value: source.DatasetID},
		{Name: "table_name", Value: source.TableName},
		{Name: "billing_id", Value: source.BillingAccountID},
	}

	job, err := q.Run(ctx)
	if err != nil {
		return false, fmt.Errorf("inserting source: %w", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return false, fmt.Errorf("waiting for source insert: %w", err)
	}
	if status.Err() != nil {
		return false, fmt.Errorf("source insert failed: %w", status.Err())
	}
	return true, nil
}

// ListSources returns all sources from the metadata table.
func (m *SourceManager) ListSources(ctx context.Context, activeOnly bool) ([]SourceRecord, error) {
	query := fmt.Sprintf("SELECT * FROM `%s.%s.billing_export_sources`", m.ProjectID, m.DatasetID)
	if activeOnly {
		query += " WHERE is_active = TRUE"
	}
	query += " ORDER BY added_at"

	q := m.Client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("querying sources: %w", err)
	}

	var records []SourceRecord
	for {
		var r SourceRecord
		err := it.Next(&r)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading source record: %w", err)
		}
		records = append(records, r)
	}
	return records, nil
}

func (m *SourceManager) sourceExists(ctx context.Context, project, dataset, table string) (bool, error) {
	query := fmt.Sprintf(`
SELECT COUNT(*) as cnt FROM %s.%s.billing_export_sources
WHERE source_project = @project AND source_dataset = @dataset AND source_table = @table_name`,
		m.ProjectID, m.DatasetID)

	q := m.Client.Query(query)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "project", Value: project},
		{Name: "dataset", Value: dataset},
		{Name: "table_name", Value: table},
	}

	it, err := q.Read(ctx)
	if err != nil {
		return false, fmt.Errorf("checking source existence: %w", err)
	}

	var row []bigquery.Value
	err = it.Next(&row)
	if err != nil {
		return false, fmt.Errorf("reading existence check: %w", err)
	}
	count, ok := row[0].(int64)
	if !ok {
		return false, fmt.Errorf("unexpected count type")
	}
	return count > 0, nil
}

// BuildSourcesFullRef returns the fully qualified table reference for a source.
func BuildSourcesFullRef(source SourceRecord) string {
	return fmt.Sprintf("`%s.%s.%s`", source.SourceProject, source.SourceDataset, source.SourceTable)
}
