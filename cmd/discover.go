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
	Short: "Discover billing export tables across accessible GCP projects",
	Long: `Scans all accessible projects for BigQuery billing export tables.
If --org-id is provided, it is logged for context but all accessible projects are scanned regardless.
Outputs discovered sources as JSON for use with the add-source command.`,
	RunE: runDiscover,
}

var (
	orgID            string
	outputFile       string
	discoverDetailed bool
	projectsFile     string
)

func init() {
	discoverCmd.Flags().StringVar(&orgID, "org-id", "", "GCP organization ID (optional, scans all accessible projects if omitted)")
	discoverCmd.Flags().StringVar(&outputFile, "output", "", "Output file path (defaults to stdout)")
	discoverCmd.Flags().BoolVar(&discoverDetailed, "detailed", false, "Output detailed/resource-level exports instead of standard")
	discoverCmd.Flags().StringVar(&projectsFile, "projects-file", "", "JSON file with project list to scan (only those projects are scanned)")
	rootCmd.AddCommand(discoverCmd)
}

func runDiscover(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	factory := &bq.ClientFactory{SAKeyFile: cfg.SAKeyFile}
	discoverer := &bq.GCPTableDiscoverer{Factory: factory}

	var lister bq.ProjectLister
	if projectsFile != "" {
		projectIDs, err := loadProjectIDsFromFile(projectsFile)
		if err != nil {
			return fmt.Errorf("loading projects file: %w", err)
		}
		lister = &bq.StaticProjectLister{ProjectIDs: projectIDs}
	} else {
		lister = &bq.GCPProjectLister{SAKeyFile: cfg.SAKeyFile}
	}

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

// loadProjectIDsFromFile reads a sources JSON file and extracts unique project IDs.
func loadProjectIDsFromFile(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	var sources []bq.BillingExportSource
	if err := json.Unmarshal(data, &sources); err != nil {
		return nil, fmt.Errorf("parsing JSON: %w", err)
	}

	seen := make(map[string]bool)
	var projectIDs []string
	for _, s := range sources {
		if !seen[s.ProjectID] {
			seen[s.ProjectID] = true
			projectIDs = append(projectIDs, s.ProjectID)
		}
	}
	return projectIDs, nil
}
