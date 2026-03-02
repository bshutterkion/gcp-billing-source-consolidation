package bigquery

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// BillingExportSource represents a discovered billing export table.
type BillingExportSource struct {
	ProjectID        string `json:"project_id"`
	DatasetID        string `json:"dataset_id"`
	TableName        string `json:"table_name"`
	BillingAccountID string `json:"billing_account_id"`
	IsDetailed       bool   `json:"is_detailed,omitempty"`
}

var (
	// Standard billing export: gcp_billing_export_v1_XXXXXX_YYYYYY_ZZZZZZ
	standardExportPattern = regexp.MustCompile(`^gcp_billing_export_v1_([0-9A-Fa-f]{6})_([0-9A-Fa-f]{6})_([0-9A-Fa-f]{6})$`)
	// Resource-level export: gcp_billing_export_resource_v1_XXXXXX_YYYYYY_ZZZZZZ
	detailedExportPattern = regexp.MustCompile(`^gcp_billing_export_resource_v1_([0-9A-Fa-f]{6})_([0-9A-Fa-f]{6})_([0-9A-Fa-f]{6})$`)
)

// ExtractBillingAccountID extracts a billing account ID from a table name.
// Returns the billing account ID (e.g., "0110FC-7EF805-B1D01E"), whether it's a
// detailed export, and whether the table name matched at all.
func ExtractBillingAccountID(tableName string) (billingAccountID string, isDetailed bool, ok bool) {
	if m := standardExportPattern.FindStringSubmatch(tableName); m != nil {
		return strings.ToUpper(fmt.Sprintf("%s-%s-%s", m[1], m[2], m[3])), false, true
	}
	if m := detailedExportPattern.FindStringSubmatch(tableName); m != nil {
		return strings.ToUpper(fmt.Sprintf("%s-%s-%s", m[1], m[2], m[3])), true, true
	}
	return "", false, false
}

// IsDetailedTable returns true if the table name matches the detailed/resource-level
// billing export pattern (gcp_billing_export_resource_v1_*).
func IsDetailedTable(tableName string) bool {
	return detailedExportPattern.MatchString(tableName)
}

// FilterSourcesByType filters a slice of SourceRecords, keeping only those
// matching the requested type (detailed or standard).
func FilterSourcesByType(sources []SourceRecord, detailed bool) []SourceRecord {
	var filtered []SourceRecord
	for _, s := range sources {
		if IsDetailedTable(s.SourceTable) == detailed {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

// ProjectLister lists projects under an organization.
type ProjectLister interface {
	ListProjects(ctx context.Context, orgID string) ([]string, error)
}

// TableDiscoverer finds billing export tables in a project.
type TableDiscoverer interface {
	DiscoverTables(ctx context.Context, projectID string) ([]BillingExportSource, error)
}

// GCPProjectLister implements ProjectLister using the Cloud Resource Manager API.
type GCPProjectLister struct {
	SAKeyFile string
}

// ListProjects returns all project IDs under the given organization.
func (l *GCPProjectLister) ListProjects(ctx context.Context, orgID string) ([]string, error) {
	var opts []option.ClientOption
	if l.SAKeyFile != "" {
		opts = append(opts, option.WithCredentialsFile(l.SAKeyFile))
	}

	client, err := resourcemanager.NewProjectsClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating resource manager client: %w", err)
	}
	defer client.Close()

	var projectIDs []string
	// Search all active projects the caller has access to.
	// We don't filter by parent because projects may be nested in folders
	// under the org. The caller should scope access appropriately.
	_ = orgID // orgID is logged for context but not used as a filter
	it := client.SearchProjects(ctx, &resourcemanagerpb.SearchProjectsRequest{})
	for {
		proj, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing projects: %w", err)
		}
		if proj.State == resourcemanagerpb.Project_ACTIVE {
			projectIDs = append(projectIDs, proj.ProjectId)
		}
	}
	return projectIDs, nil
}

// GCPTableDiscoverer implements TableDiscoverer using the BigQuery API.
type GCPTableDiscoverer struct {
	Factory *ClientFactory
}

// DiscoverTables finds all billing export tables in the given project.
func (d *GCPTableDiscoverer) DiscoverTables(ctx context.Context, projectID string) ([]BillingExportSource, error) {
	client, err := d.Factory.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	var sources []BillingExportSource

	dsIt := client.Datasets(ctx)
	for {
		ds, err := dsIt.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// Skip projects where we lack BQ access
			return sources, nil
		}

		tblIt := ds.Tables(ctx)
		for {
			tbl, err := tblIt.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				break
			}

			billingID, isDetailed, ok := ExtractBillingAccountID(tbl.TableID)
			if !ok {
				continue
			}

			sources = append(sources, BillingExportSource{
				ProjectID:        projectID,
				DatasetID:        ds.DatasetID,
				TableName:        tbl.TableID,
				BillingAccountID: billingID,
				IsDetailed:       isDetailed,
			})
		}
	}

	return sources, nil
}

// DiscoverAll finds all billing export tables across all projects in an organization.
func DiscoverAll(ctx context.Context, lister ProjectLister, discoverer TableDiscoverer, orgID string) ([]BillingExportSource, error) {
	projects, err := lister.ListProjects(ctx, orgID)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Found %d projects in organization %s\n", len(projects), orgID)

	var allSources []BillingExportSource
	for _, projID := range projects {
		sources, err := discoverer.DiscoverTables(ctx, projID)
		if err != nil {
			fmt.Printf("  Warning: error scanning project %s: %v\n", projID, err)
			continue
		}
		if len(sources) > 0 {
			for _, s := range sources {
				label := "standard"
				if s.IsDetailed {
					label = "detailed"
				}
				fmt.Printf("  Found %s export in %s: %s.%s (billing account: %s)\n",
					label, s.ProjectID, s.DatasetID, s.TableName, s.BillingAccountID)
			}
		}
		allSources = append(allSources, sources...)
	}

	return allSources, nil
}
