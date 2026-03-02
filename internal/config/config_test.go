package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidate_MissingProject(t *testing.T) {
	c := &Config{Dataset: "ds"}
	err := c.Validate()
	assert.EqualError(t, err, "--project is required")
}

func TestValidate_MissingDataset(t *testing.T) {
	c := &Config{Project: "proj"}
	err := c.Validate()
	assert.EqualError(t, err, "--dataset is required")
}

func TestValidate_DefaultTable(t *testing.T) {
	c := &Config{Project: "proj", Dataset: "ds"}
	err := c.Validate()
	assert.NoError(t, err)
	assert.Equal(t, DefaultTable, c.Table)
}

func TestValidate_CustomTable(t *testing.T) {
	c := &Config{Project: "proj", Dataset: "ds", Table: "custom"}
	err := c.Validate()
	assert.NoError(t, err)
	assert.Equal(t, "custom", c.Table)
}
