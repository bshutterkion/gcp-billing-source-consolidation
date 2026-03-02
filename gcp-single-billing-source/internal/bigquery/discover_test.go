package bigquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractBillingAccountID_Standard(t *testing.T) {
	id, isDetailed, ok := ExtractBillingAccountID("gcp_billing_export_v1_0110FC_7EF805_B1D01E")
	assert.True(t, ok)
	assert.False(t, isDetailed)
	assert.Equal(t, "0110FC-7EF805-B1D01E", id)
}

func TestExtractBillingAccountID_Detailed(t *testing.T) {
	id, isDetailed, ok := ExtractBillingAccountID("gcp_billing_export_resource_v1_01C50B_15317E_A3F7AE")
	assert.True(t, ok)
	assert.True(t, isDetailed)
	assert.Equal(t, "01C50B-15317E-A3F7AE", id)
}

func TestExtractBillingAccountID_NoMatch(t *testing.T) {
	_, _, ok := ExtractBillingAccountID("some_other_table")
	assert.False(t, ok)
}

func TestExtractBillingAccountID_LowercaseHex(t *testing.T) {
	id, _, ok := ExtractBillingAccountID("gcp_billing_export_v1_0110fc_7ef805_b1d01e")
	assert.True(t, ok)
	assert.Equal(t, "0110FC-7EF805-B1D01E", id)
}

func TestFilterSourcesByType(t *testing.T) {
	sources := []SourceRecord{
		{SourceTable: "gcp_billing_export_v1_0110FC_7EF805_B1D01E"},
		{SourceTable: "gcp_billing_export_resource_v1_01C50B_15317E_A3F7AE"},
		{SourceTable: "gcp_billing_export_v1_01C50B_15317E_A3F7AE"},
	}

	standard := FilterSourcesByType(sources, false)
	assert.Len(t, standard, 2)

	detailed := FilterSourcesByType(sources, true)
	assert.Len(t, detailed, 1)
	assert.Equal(t, "gcp_billing_export_resource_v1_01C50B_15317E_A3F7AE", detailed[0].SourceTable)
}

func TestFilterSourcesByType_Empty(t *testing.T) {
	sources := []SourceRecord{
		{SourceTable: "gcp_billing_export_v1_0110FC_7EF805_B1D01E"},
	}
	detailed := FilterSourcesByType(sources, true)
	assert.Empty(t, detailed)
}

func TestIsDetailedTable(t *testing.T) {
	assert.False(t, IsDetailedTable("gcp_billing_export_v1_0110FC_7EF805_B1D01E"))
	assert.True(t, IsDetailedTable("gcp_billing_export_resource_v1_01C50B_15317E_A3F7AE"))
	assert.False(t, IsDetailedTable("some_other_table"))
}

// Mock implementations for unit testing

type mockProjectLister struct {
	projects []string
	err      error
}

func (m *mockProjectLister) ListProjects(_ context.Context, _ string) ([]string, error) {
	return m.projects, m.err
}

type mockTableDiscoverer struct {
	tables map[string][]BillingExportSource
	err    error
}

func (m *mockTableDiscoverer) DiscoverTables(_ context.Context, projectID string) ([]BillingExportSource, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.tables[projectID], nil
}

func TestDiscoverAll(t *testing.T) {
	lister := &mockProjectLister{
		projects: []string{"proj-a", "proj-b"},
	}
	discoverer := &mockTableDiscoverer{
		tables: map[string][]BillingExportSource{
			"proj-a": {
				{ProjectID: "proj-a", DatasetID: "billing", TableName: "gcp_billing_export_v1_0110FC_7EF805_B1D01E", BillingAccountID: "0110FC-7EF805-B1D01E"},
			},
			"proj-b": {
				{ProjectID: "proj-b", DatasetID: "billing", TableName: "gcp_billing_export_v1_01C50B_15317E_A3F7AE", BillingAccountID: "01C50B-15317E-A3F7AE"},
			},
		},
	}

	sources, err := DiscoverAll(context.Background(), lister, discoverer, "123")
	require.NoError(t, err)
	assert.Len(t, sources, 2)
	assert.Equal(t, "0110FC-7EF805-B1D01E", sources[0].BillingAccountID)
	assert.Equal(t, "01C50B-15317E-A3F7AE", sources[1].BillingAccountID)
}

func TestDiscoverAll_EmptyProjects(t *testing.T) {
	lister := &mockProjectLister{projects: []string{}}
	discoverer := &mockTableDiscoverer{tables: map[string][]BillingExportSource{}}

	sources, err := DiscoverAll(context.Background(), lister, discoverer, "123")
	require.NoError(t, err)
	assert.Empty(t, sources)
}
