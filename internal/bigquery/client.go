package bigquery

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

// ClientFactory creates BigQuery clients.
type ClientFactory struct {
	SAKeyFile string
}

// CredentialOpts returns a ClientOption for the SA key file if it exists,
// otherwise returns nil to fall back to Application Default Credentials.
func CredentialOpts(saKeyFile string) []option.ClientOption {
	if saKeyFile != "" {
		if info, err := os.Stat(saKeyFile); err == nil && !info.IsDir() {
			return []option.ClientOption{option.WithCredentialsFile(saKeyFile)}
		}
	}
	return nil
}

// NewClient creates a BigQuery client for the given project.
// Uses Application Default Credentials unless SAKeyFile points to an existing file.
func (f *ClientFactory) NewClient(ctx context.Context, projectID string) (*bigquery.Client, error) {
	opts := CredentialOpts(f.SAKeyFile)

	client, err := bigquery.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating BigQuery client for project %s: %w", projectID, err)
	}
	return client, nil
}
