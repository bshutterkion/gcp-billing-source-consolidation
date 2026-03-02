package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	bq "github.com/kionsoftware/billing-consolidator/internal/bigquery"
	"github.com/spf13/cobra"
)

var discoverCmd = &cobra.Command{
	Use:   "discover",
	Short: "Discover billing export tables across a GCP organization",
	Long: `Scans all projects in a GCP organization for BigQuery billing export tables.
Outputs discovered sources as JSON for use with the add-source command.`,
	RunE: runDiscover,
}

var (
	orgID           string
	outputFile      string
	discoverDetailed bool
)

func init() {
	discoverCmd.Flags().StringVar(&orgID, "org-id", "", "GCP organization ID (required)")
	discoverCmd.Flags().StringVar(&outputFile, "output", "", "Output file path (defaults to stdout)")
	discoverCmd.Flags().BoolVar(&discoverDetailed, "detailed", false, "Output detailed/resource-level exports instead of standard")
	_ = discoverCmd.MarkFlagRequired("org-id")
	rootCmd.AddCommand(discoverCmd)
}

func runDiscover(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	lister := &bq.GCPProjectLister{SAKeyFile: cfg.SAKeyFile}
	factory := &bq.ClientFactory{SAKeyFile: cfg.SAKeyFile}
	discoverer := &bq.GCPTableDiscoverer{Factory: factory}

	sources, err := bq.DiscoverAll(ctx, lister, discoverer, orgID)
	if err != nil {
		return fmt.Errorf("discovery failed: %w", err)
	}

	// Separate standard and detailed exports
	var standard, detailed []bq.BillingExportSource
	for _, s := range sources {
		if s.IsDetailed {
			detailed = append(detailed, s)
		} else {
			standard = append(standard, s)
		}
	}

	fmt.Printf("\nDiscovered %d standard exports and %d detailed exports\n", len(standard), len(detailed))

	selected := standard
	if discoverDetailed {
		selected = detailed
	}

	if len(selected) == 0 {
		kind := "standard"
		if discoverDetailed {
			kind = "detailed"
		}
		fmt.Printf("No %s exports found.\n", kind)
		return nil
	}

	data, err := json.MarshalIndent(selected, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling sources: %w", err)
	}

	if outputFile != "" {
		if err := os.WriteFile(outputFile, data, 0644); err != nil {
			return fmt.Errorf("writing output file: %w", err)
		}
		fmt.Printf("Wrote %d sources to %s\n", len(selected), outputFile)
	} else {
		fmt.Println(string(data))
	}

	return nil
}
