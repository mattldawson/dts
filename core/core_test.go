// Genesearch: a Gene Homology Search Service (Genesearch)
// Copyright (c) 2021, Cohere Consulting, LLC.  All rights reserved.
//
// If you have questions about your rights to use or distribute this software,
// please contact Berkeley Lab's Intellectual Property Office at
// IPO@lbl.gov.
//
// NOTICE.  This Software was developed under funding from the U.S. Department
// of Energy and the U.S. Government consequently retains certain rights.  As
// such, the U.S. Government has been granted for itself and others acting on
// its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the
// Software to reproduce, distribute copies to the public, prepare derivative
// works, and perform publicly and display publicly, and to permit others to do
// so.

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
	assert.Greater(t, uptime, 0.0, "Uptme is non-positive.")
}
