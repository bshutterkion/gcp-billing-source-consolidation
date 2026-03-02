package cmd

import (
	"context"
	"fmt"
	"text/tabwriter"
	"os"

	bq "github.com/kionsoftware/billing-consolidator/internal/bigquery"
	"github.com/spf13/cobra"
)

var listSourcesCmd = &cobra.Command{
	Use:   "list-sources",
	Short: "List registered billing export sources",
	Long:  `Displays all billing export sources registered in the metadata table.`,
	RunE:  runListSources,
}

var showAll bool

func init() {
	listSourcesCmd.Flags().BoolVar(&showAll, "all", false, "Show inactive sources too")
	rootCmd.AddCommand(listSourcesCmd)
}

func runListSources(cmd *cobra.Command, args []string) error {
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

	sources, err := mgr.ListSources(ctx, !showAll)
	if err != nil {
		return err
	}

	if len(sources) == 0 {
		fmt.Println("No sources registered.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "PROJECT\tDATASET\tTABLE\tBILLING ACCOUNT\tACTIVE\tADDED")
	for _, s := range sources {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%v\t%s\n",
			s.SourceProject, s.SourceDataset, s.SourceTable,
			s.BillingAccountID, s.IsActive, s.AddedAt.Format("2006-01-02 15:04:05"))
	}
	w.Flush()

	fmt.Printf("\nTotal: %d sources\n", len(sources))
	return nil
}
