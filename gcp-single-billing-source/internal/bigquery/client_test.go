package bigquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientFactory_DefaultADC(t *testing.T) {
	f := &ClientFactory{}
	assert.Empty(t, f.SAKeyFile)
}

func TestClientFactory_WithSAKey(t *testing.T) {
	f := &ClientFactory{SAKeyFile: "/path/to/key.json"}
	assert.Equal(t, "/path/to/key.json", f.SAKeyFile)
}
