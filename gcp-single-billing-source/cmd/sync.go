package cmd

import (
	"context"
	"fmt"

	bq "github.com/kionsoftware/billing-consolidator/internal/bigquery"
	"github.com/kionsoftware/billing-consolidator/internal/config"
	"github.com/spf13/cobra"
)

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Consolidate billing exports into a single table",
	Long: `Runs a full load or incremental sync of all registered billing export sources
into the consolidated table.

Full mode (default): Creates or replaces the consolidated table with data from all sources.
Incremental mode: Deletes and re-inserts data for the last N days (default 3).`,
	RunE: runSync,
}

var (
	incremental  bool
	days         int
	dryRun       bool
	syncDetailed bool
)

func init() {
	syncCmd.Flags().BoolVar(&incremental, "incremental", false, "Run incremental sync instead of full load")
	syncCmd.Flags().IntVar(&days, "days", 3, "Number of days to look back for incremental sync")
	syncCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Print SQL without executing")
	syncCmd.Flags().BoolVar(&syncDetailed, "detailed", false, "Sync detailed/resource-level exports instead of standard")
	rootCmd.AddCommand(syncCmd)
}

func runSync(cmd *cobra.Command, args []string) error {
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

	// Get active sources
	mgr := &bq.SourceManager{
		Client:    client,
		ProjectID: cfg.Project,
		DatasetID: cfg.Dataset,
	}

	allSources, err := mgr.ListSources(ctx, true)
	if err != nil {
		return fmt.Errorf("listing sources: %w", err)
	}

	sources := bq.FilterSourcesByType(allSources, syncDetailed)

	kind := "standard"
	if syncDetailed {
		kind = "detailed"
	}

	if len(sources) == 0 {
		return fmt.Errorf("no active %s sources found — run 'add-source' first", kind)
	}

	fmt.Printf("Found %d active %s sources (of %d total)\n", len(sources), kind, len(allSources))

	consolidator := &bq.Consolidator{
		Client:    client,
		ProjectID: cfg.Project,
		DatasetID: cfg.Dataset,
		TableName: config.DefaultTable,
	}

	if incremental {
		return consolidator.RunIncrementalSync(ctx, sources, days, dryRun)
	}
	return consolidator.RunFullLoad(ctx, sources, dryRun)
}
