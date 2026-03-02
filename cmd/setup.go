package cmd

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	bq "github.com/kionsoftware/billing-consolidator/internal/bigquery"
	"github.com/kionsoftware/billing-consolidator/internal/config"
	"github.com/spf13/cobra"
)

var setupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Create destination dataset and metadata table",
	Long: `Creates the destination BigQuery dataset (if it doesn't exist) and the
billing_export_sources metadata table used to track source exports.
This command is idempotent and safe to run multiple times.`,
	RunE: runSetup,
}

func init() {
	rootCmd.AddCommand(setupCmd)
}

func runSetup(cmd *cobra.Command, args []string) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	ctx := context.Background()
	factory := &bq.ClientFactory{SAKeyFile: cfg.SAKeyFile}

	client, err := factory.NewClient(ctx, cfg.Project)
	if err != nil {
		return err
	}
	defer client.Close()

	// Create dataset if it doesn't exist
	fmt.Printf("Creating dataset %s.%s (if not exists)...\n", cfg.Project, cfg.Dataset)
	ds := client.Dataset(cfg.Dataset)
	if err := ds.Create(ctx, &bigquery.DatasetMetadata{
		Location: "US",
	}); err != nil {
		// Ignore "already exists" errors
		if !isAlreadyExistsError(err) {
			return fmt.Errorf("creating dataset: %w", err)
		}
		fmt.Println("  Dataset already exists.")
	} else {
		fmt.Println("  Dataset created.")
	}

	// Create metadata table
	fmt.Printf("Creating metadata table %s...\n", config.MetadataTable)
	mgr := &bq.SourceManager{
		Client:    client,
		ProjectID: cfg.Project,
		DatasetID: cfg.Dataset,
	}
	if err := mgr.CreateMetadataTable(ctx); err != nil {
		return err
	}
	fmt.Println("  Metadata table ready.")

	fmt.Println("\nSetup complete. Next steps:")
	fmt.Printf("  1. Run 'billing-consolidator discover --org-id ORG_ID --output sources.json'\n")
	fmt.Printf("  2. Run 'billing-consolidator add-source --project %s --dataset %s --from-file sources.json'\n", cfg.Project, cfg.Dataset)

	return nil
}

func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	return fmt.Sprintf("%v", err) != "" && (
		contains(err.Error(), "Already Exists") ||
		contains(err.Error(), "already exists") ||
		contains(err.Error(), "409"))
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsStr(s, substr))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
