package endpoints

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kbase/dts/config"
)

const globusConfig string = `
endpoints:
  globus:
    name: NERSC DTN
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
  invalid:
    name: NERSC DTN
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
`

// this function gets called at the begіnning of a test session
func setup() {
	config.Init([]byte(globusConfig))
}

// this function gets called after all tests have been run
func breakdown() {
}

func TestNewGlobusEndpoint(t *testing.T) {
	assert := assert.New(t)
	ep, err := NewEndpoint("globus")
	assert.NotNil(ep, "Globus endpoint not created")
	assert.Nil(err, "Globus endpoint creation encountered an error")
}

func TestInvalidEndpoint(t *testing.T) {
	assert := assert.New(t)
	ep, err := NewEndpoint("invalid")
	assert.Nil(ep, "Invalid endpoint somehow created")
	assert.NotNil(err, "Invalid endpoint creation returned no error")
}

func TestNonexistentEndpoint(t *testing.T) {
	assert := assert.New(t)
	ep, err := NewEndpoint("nonexistent")
	assert.Nil(ep, "Nonexistent endpoint somehow created")
	assert.NotNil(err, "Nonexistent endpoint creation returned no error")
}

// this runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	var status int
	setup()
	status = m.Run()
	breakdown()
	os.Exit(status)
}
