package cmd

import (
	"context"
	"fmt"

	bq "github.com/kionsoftware/billing-consolidator/internal/bigquery"
	"github.com/kionsoftware/billing-consolidator/internal/config"
	"github.com/spf13/cobra"
)

var scheduleCmd = &cobra.Command{
	Use:   "schedule",
	Short: "Create or update a BQ scheduled query for daily sync",
	Long: `Creates a BigQuery Data Transfer scheduled query that runs the incremental
sync SQL on a cron schedule. The scheduled query is idempotent — if one with
the same display name exists, it will be updated.

Note: After adding new sources, re-run this command to update the scheduled query SQL.`,
	RunE: runSchedule,
}

var (
	scheduleCron       string
	scheduleDays       int
	displayName        string
	scheduleDryRun     bool
	serviceAccountName string
	scheduleDetailed   bool
)

func init() {
	scheduleCmd.Flags().StringVar(&scheduleCron, "cron", "every 24 hours", "Schedule in BQ Data Transfer format (e.g., 'every 24 hours')")
	scheduleCmd.Flags().IntVar(&scheduleDays, "days", 3, "Number of days to look back")
	scheduleCmd.Flags().StringVar(&displayName, "display-name", "billing-export-consolidation", "Display name for the scheduled query")
	scheduleCmd.Flags().BoolVar(&scheduleDryRun, "dry-run", false, "Print SQL without creating the schedule")
	scheduleCmd.Flags().StringVar(&serviceAccountName, "service-account", "", "Service account email to run the scheduled query as (required for ADC auth)")
	scheduleCmd.Flags().BoolVar(&scheduleDetailed, "detailed", false, "Schedule sync for detailed/resource-level exports instead of standard")
	rootCmd.AddCommand(scheduleCmd)
}

func runSchedule(cmd *cobra.Command, args []string) error {
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

	sources := bq.FilterSourcesByType(allSources, scheduleDetailed)

	kind := "standard"
	if scheduleDetailed {
		kind = "detailed"
	}

	if len(sources) == 0 {
		return fmt.Errorf("no active %s sources found — run 'add-source' first", kind)
	}

	// Discover schemas for explicit column handling
	consolidator := &bq.Consolidator{
		Client:    client,
		ProjectID: cfg.Project,
		DatasetID: cfg.Dataset,
		TableName: config.DefaultTable,
	}
	fmt.Println("Discovering source schemas...")
	merged, schemas, validSources, err := consolidator.DiscoverAndMergeSchemas(ctx, sources)
	if err != nil {
		return fmt.Errorf("discovering schemas: %w", err)
	}

	// Build the scheduled query SQL
	sql, err := bq.BuildScheduledQuerySQL(cfg.Project, cfg.Dataset, config.DefaultTable, validSources, scheduleDays, merged, schemas)
	if err != nil {
		return err
	}

	if scheduleDryRun {
		fmt.Println("-- DRY RUN: Scheduled query SQL --")
		fmt.Println(sql)
		fmt.Printf("\n-- Schedule: %s\n", scheduleCron)
		fmt.Printf("-- Display name: %s\n", displayName)
		return nil
	}

	// Create or update the scheduled query
	scheduler := &bq.Scheduler{SAKeyFile: cfg.SAKeyFile}
	configName, err := scheduler.CreateOrUpdateSchedule(ctx, cfg.Project, cfg.Dataset, displayName, sql, scheduleCron, serviceAccountName)
	if err != nil {
		return err
	}

	fmt.Printf("Scheduled query configured: %s\n", configName)
	fmt.Printf("  Schedule: %s\n", scheduleCron)
	fmt.Printf("  Lookback: %d days\n", scheduleDays)
	fmt.Printf("  Sources: %d\n", len(sources))
	fmt.Println("\nThe scheduled query will appear in the BQ console under Data Transfers.")
	fmt.Println("Re-run this command after adding new sources to update the SQL.")

	return nil
}
