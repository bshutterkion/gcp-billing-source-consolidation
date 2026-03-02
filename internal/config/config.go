package config

import "fmt"

// Config holds the destination project/dataset configuration.
type Config struct {
	Project   string
	Dataset   string
	Table     string
	SAKeyFile string
}

// DefaultTable is the name of the consolidated billing export table.
const DefaultTable = "gcp_billing_export_consolidated"

// MetadataTable is the name of the source registry table.
const MetadataTable = "billing_export_sources"

// Validate checks that required fields are set.
func (c *Config) Validate() error {
	if c.Project == "" {
		return fmt.Errorf("--project is required")
	}
	if c.Dataset == "" {
		return fmt.Errorf("--dataset is required")
	}
	if c.Table == "" {
		c.Table = DefaultTable
	}
	return nil
}
