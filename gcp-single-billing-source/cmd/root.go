package cmd

import (
	"fmt"
	"os"

	"github.com/kionsoftware/billing-consolidator/internal/config"
	"github.com/spf13/cobra"
)

var cfg config.Config

var rootCmd = &cobra.Command{
	Use:   "billing-consolidator",
	Short: "Consolidate multiple GCP billing exports into a single BigQuery table",
	Long: `billing-consolidator discovers, consolidates, and maintains a unified BigQuery
billing export table from multiple GCP billing accounts. Designed for use with
Kion's billing source configuration.`,
}

// Execute runs the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfg.Project, "project", "", "Destination GCP project ID")
	rootCmd.PersistentFlags().StringVar(&cfg.Dataset, "dataset", "", "Destination BigQuery dataset ID")
	rootCmd.PersistentFlags().StringVar(&cfg.SAKeyFile, "sa-key", "", "Path to service account JSON key file (defaults to ADC)")
}
