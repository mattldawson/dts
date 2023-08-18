package globus

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"dts/config"
)

const globusConfig string = `
globus:
  auth:
    client_id: ${DTS_GLOBUS_CLIENT_ID}
    client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
  endpoints:
    globus-jdp:
      name: NERSC DTN
      id: ${DTS_GLOBUS_TEST_ENDPOINT}
`

// this function gets called at the begіnning of a test session
func setup() {
	config.Init([]byte(globusConfig))
}

// this function gets called after all tests have been run
func breakdown() {
}

func TestGlobusConstructor(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	endpoint, err := NewEndpoint("globus-jdp")
	assert.NotNil(endpoint)
	assert.Nil(err)
}

func TestGlobusTransfers(t *testing.T) {
	assert := assert.New(t) // binds assert to t
	endpoint, _ := NewEndpoint("globus-jdp")
	xfers, err := endpoint.Transfers()
	assert.NotNil(xfers)
	assert.Nil(err)
}

// This runs setup, runs all tests, and does breakdown.
func TestMain(m *testing.M) {
	var status int
	setup()
	status = m.Run()
	breakdown()
	os.Exit(status)
}
