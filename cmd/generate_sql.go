package cmd

import (
	"fmt"
	"os"

	"github.com/kionsoftware/billing-consolidator/internal/parser"
	"github.com/spf13/cobra"
)

var generateSQLCmd = &cobra.Command{
	Use:   "generate-sql",
	Short: "Generate consolidation SQL from xlsx exports and a CSV mapping (no GCP access needed)",
	Long: `Generates BigQuery consolidation SQL offline using:
  1. A zip file containing xlsx billing export samples (one per billing account)
  2. A CSV mapping file: billing_account_id,project_id,dataset_id

This avoids needing GCP API access to discover schemas. The xlsx column headers
are used only to detect mismatches; the generated SQL uses SELECT * since BQ
schema is authoritative.

Output includes two SQL blocks:
  - Full load: CREATE OR REPLACE TABLE ... PARTITION BY ... AS (SELECT ... UNION ALL ...)
  - Incremental sync: DECLARE cutoff ... DELETE ... INSERT ...`,
	RunE: runGenerateSQL,
}

var (
	zipPath     string
	mappingPath string
	destProject string
	destDataset string
	genDays     int
	outputPath  string
)

func init() {
	generateSQLCmd.Flags().StringVar(&zipPath, "zip", "", "Path to zip file containing xlsx billing exports")
	generateSQLCmd.Flags().StringVar(&mappingPath, "mapping", "", "Path to CSV mapping file (billing_account_id,project_id,dataset_id)")
	generateSQLCmd.Flags().StringVar(&destProject, "dest-project", "", "Destination GCP project ID")
	generateSQLCmd.Flags().StringVar(&destDataset, "dest-dataset", "billing_consolidated", "Destination BigQuery dataset ID")
	generateSQLCmd.Flags().IntVar(&genDays, "days", 3, "Lookback period in days for incremental sync")
	generateSQLCmd.Flags().StringVarP(&outputPath, "output", "o", "consolidated.sql", "Output file path for generated SQL")

	_ = generateSQLCmd.MarkFlagRequired("zip")
	_ = generateSQLCmd.MarkFlagRequired("mapping")
	_ = generateSQLCmd.MarkFlagRequired("dest-project")

	rootCmd.AddCommand(generateSQLCmd)
}

func runGenerateSQL(cmd *cobra.Command, args []string) error {
	// Read mapping CSV
	fmt.Println("Reading mapping CSV...")
	mapping, err := parser.ReadMapping(mappingPath)
	if err != nil {
		return err
	}
	fmt.Printf("Loaded %d billing account mappings\n", len(mapping))

	// Parse zip of xlsx files
	fmt.Println("\nParsing xlsx exports from zip...")
	sources, err := parser.ParseZip(zipPath, mapping)
	if err != nil {
		return err
	}
	fmt.Printf("\nFound %d billing export sources\n", len(sources))

	// Check for column mismatches
	if parser.ColumnsMatch(sources) {
		fmt.Println("All sources have identical column headers.")
	} else {
		parser.PrintColumnWarnings(sources)
	}

	destTable := "gcp_billing_export_consolidated"

	// Generate full load SQL
	fullSQL := parser.BuildFullLoadSQL(destProject, destDataset, destTable, sources)

	// Generate incremental sync SQL
	incrSQL := parser.BuildIncrementalSQL(destProject, destDataset, destTable, sources, genDays)

	// Write to file
	content := separator("FULL LOAD SQL") + "\n" + fullSQL + "\n\n" +
		separator("INCREMENTAL SYNC SQL (for scheduled query)") + "\n" + incrSQL + "\n"

	if err := os.WriteFile(outputPath, []byte(content), 0644); err != nil {
		return fmt.Errorf("writing output file: %w", err)
	}
	fmt.Printf("\nSQL written to %s\n", outputPath)

	return nil
}

func separator(label string) string {
	return fmt.Sprintf("-- ========== %s ==========", label)
}
