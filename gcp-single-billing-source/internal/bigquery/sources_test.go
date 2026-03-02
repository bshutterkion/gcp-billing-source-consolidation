package bigquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildSourcesFullRef(t *testing.T) {
	tests := []struct {
		name     string
		source   SourceRecord
		expected string
	}{
		{
			name: "basic",
			source: SourceRecord{
				SourceProject: "proj-a",
				SourceDataset: "billing",
				SourceTable:   "gcp_billing_export_v1_0110FC_7EF805_B1D01E",
			},
			expected: "`proj-a.billing.gcp_billing_export_v1_0110FC_7EF805_B1D01E`",
		},
		{
			name: "different project",
			source: SourceRecord{
				SourceProject: "proj-b",
				SourceDataset: "dataset_2",
				SourceTable:   "gcp_billing_export_v1_01C50B_15317E_A3F7AE",
			},
			expected: "`proj-b.dataset_2.gcp_billing_export_v1_01C50B_15317E_A3F7AE`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildSourcesFullRef(tt.source)
			assert.Equal(t, tt.expected, result)
		})
	}
}
