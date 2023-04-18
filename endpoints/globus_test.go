package endpoints

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"dts/config"
)

const globusConfig string = `
globus:
  auth:
    client_id: ${GLOBUS_CLIENT_ID}
    client_secret: ${GLOBUS_CLIENT_SECRET}
  endpoints:
    globus-jdp:
      name: NERSC DTN
      id: 9d6d994a-6d04-11e5-ba46-22000b92c6ec
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

	endpoint, err := NewGlobusEndpoint("globus-jdp")
	assert.NotNil(endpoint)
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
