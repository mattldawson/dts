// These tests verify that the core utilities work properly.
package core

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// Tests whether core.Init works once.
func TestInitOnce(t *testing.T) {
	err := Init()
	assert.Nil(t, err, "core.Init Failed!")
}

// Tests whether core.Init works twice in a row.
func TestInitTwice(t *testing.T) {
	i := 0
	for i < 2 {
		err := Init()
		assert.Nil(t, err, "core.Init Failed!")
		i++
	}
}

// Tests whether core.Uptime() reurns a positive time duration.
func TestUptime(t *testing.T) {
	Init()
	uptime := Uptime()
	assert.Greater(t, uptime, 0.0, "Uptime is non-positive.")
}
