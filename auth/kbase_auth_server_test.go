// These tests verify that we can connect to the KBase authentication server
// and access a user's ORCID credential(s). The tests require the following
// environment variables to be set:
//
// * KBASE_DEV_TOKEN: a valid unencoded KBase developer token
// * KBASE_DEV_ORCID: an ORCID identifier corresponding to the developer token
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
	devToken := os.Getenv("KBASE_DEV_TOKEN")
	server, err := NewKBaseAuthServer(devToken)
	assert.NotNil(server, "Authentication server not created")
	assert.Nil(err, "Authentication server constructor triggered an error")
}

// tests whether the authentication server can return the proper credentials
// for the owner of the developer token
func TestOrchidIds(t *testing.T) {
	assert := assert.New(t) // binds assert to t
	devToken := os.Getenv("KBASE_DEV_TOKEN")
	orcidId := os.Getenv("KBASE_DEV_ORCID")
	assert.False(orcidId == "")
	server, _ := NewKBaseAuthServer(devToken)
	orcidIds, err := server.OrcidIds()
	assert.Nil(err)
	var foundOrcidId bool
	for _, id := range orcidIds {
		if orcidId == id {
			foundOrcidId = true
			break
		}
	}
	assert.True(foundOrcidId)
}
