package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	bq "github.com/kionsoftware/billing-consolidator/internal/bigquery"
	"github.com/spf13/cobra"
)

var addSourceCmd = &cobra.Command{
	Use:   "add-source",
	Short: "Register a billing export source",
	Long: `Register one or more billing export sources into the metadata table.
Sources can be added from a JSON file (output of discover) or individually.`,
	RunE: runAddSource,
}

var (
	fromFile      string
	sourceProject string
	sourceDataset string
	sourceTable   string
)

func init() {
	addSourceCmd.Flags().StringVar(&fromFile, "from-file", "", "JSON file with discovered sources")
	addSourceCmd.Flags().StringVar(&sourceProject, "source-project", "", "Source project ID (for single source)")
	addSourceCmd.Flags().StringVar(&sourceDataset, "source-dataset", "", "Source dataset ID (for single source)")
	addSourceCmd.Flags().StringVar(&sourceTable, "source-table", "", "Source table name (for single source)")
	rootCmd.AddCommand(addSourceCmd)
}

func runAddSource(cmd *cobra.Command, args []string) error {
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

	mgr := &bq.SourceManager{
		Client:    client,
		ProjectID: cfg.Project,
		DatasetID: cfg.Dataset,
	}

	var sources []bq.BillingExportSource

	if fromFile != "" {
		data, err := os.ReadFile(fromFile)
		if err != nil {
			return fmt.Errorf("reading source file: %w", err)
		}
		if err := json.Unmarshal(data, &sources); err != nil {
			return fmt.Errorf("parsing source file: %w", err)
		}
		fmt.Printf("Loaded %d sources from %s\n", len(sources), fromFile)
	} else if sourceProject != "" && sourceDataset != "" && sourceTable != "" {
		billingID, _, ok := bq.ExtractBillingAccountID(sourceTable)
		if !ok {
			billingID = "unknown"
		}
		sources = append(sources, bq.BillingExportSource{
			ProjectID:        sourceProject,
			DatasetID:        sourceDataset,
			TableName:        sourceTable,
			BillingAccountID: billingID,
		})
	} else {
		return fmt.Errorf("either --from-file or --source-project/--source-dataset/--source-table are required")
	}

	var added, skipped int
	for _, s := range sources {
		inserted, err := mgr.AddSource(ctx, s)
		if err != nil {
			fmt.Printf("  Error adding %s.%s.%s: %v\n", s.ProjectID, s.DatasetID, s.TableName, err)
			continue
		}
		if inserted {
			fmt.Printf("  Added: %s.%s.%s (billing account: %s)\n", s.ProjectID, s.DatasetID, s.TableName, s.BillingAccountID)
			added++
		} else {
			fmt.Printf("  Skipped (duplicate): %s.%s.%s\n", s.ProjectID, s.DatasetID, s.TableName)
			skipped++
		}
	}

	fmt.Printf("\nRegistered %d new sources (%d skipped as duplicates)\n", added, skipped)
	return nil
}
