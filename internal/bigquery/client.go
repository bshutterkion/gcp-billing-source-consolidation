package bigquery

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

// ClientFactory creates BigQuery clients.
type ClientFactory struct {
	SAKeyFile string
}

// NewClient creates a BigQuery client for the given project.
// Uses Application Default Credentials unless SAKeyFile is set.
func (f *ClientFactory) NewClient(ctx context.Context, projectID string) (*bigquery.Client, error) {
	var opts []option.ClientOption
	if f.SAKeyFile != "" {
		opts = append(opts, option.WithCredentialsFile(f.SAKeyFile))
	}

	client, err := bigquery.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating BigQuery client for project %s: %w", projectID, err)
	}
	return client, nil
}
