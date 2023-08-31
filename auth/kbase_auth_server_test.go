// These tests verify that we can connect to the KBase authentication server
// and access a user's ORCID credential(s). The tests require the following
// environment variables to be set:
//
// * DTS_KBASE_DEV_TOKEN: a valid unencoded KBase developer token
// * DTS_KBASE_TEST_ORCID: a valid ORCID identifier for a KBase user
package auth

import (
	//	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// tests whether a proxy for the KBase authentication server can be
// constructed
func TestNewKBaseAuthServer(t *testing.T) {
	assert := assert.New(t) // binds assert to t
	devToken := os.Getenv("DTS_KBASE_DEV_TOKEN")
	server, err := NewKBaseAuthServer(devToken)
	assert.NotNil(server, "Authentication server not created")
	assert.Nil(err, "Authentication server constructor triggered an error")
}

// tests whether the authentication server can return the proper credentials
// for the owner of the developer token
func TestOrchids(t *testing.T) {
	assert := assert.New(t) // binds assert to t
	devToken := os.Getenv("DTS_KBASE_DEV_TOKEN")
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	assert.False(orcid == "")
	server, _ := NewKBaseAuthServer(devToken)
	orcids, err := server.Orcids()
	assert.Nil(err)
	var foundOrcid bool
	for _, id := range orcids {
		if orcid == id {
			foundOrcid = true
			break
		}
	}
	assert.True(foundOrcid)
}
