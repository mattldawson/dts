// Copyright (c) 2023 The KBase Project and its Contributors
// Copyright (c) 2023 Cohere Consulting, LLC
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
// of the Software, and to permit persons to whom the Software is furnished to do
// so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// These tests verify that we can connect to the KBase authentication server
// and access a user's ORCID credential(s). The tests require the following
// environment variables to be set:
//
// * DTS_KBASE_DEV_TOKEN: a valid unencoded KBase developer token
// * DTS_KBASE_TEST_ORCID: a valid ORCID identifier for a KBase user
package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// tests whether a proxy for the KBase authentication server can be
// constructed
func TestSetKBaseLocalUsernameForOrcid(t *testing.T) {
	assert := assert.New(t)
	err := SetKBaseLocalUsernameForOrcid("my-fake-orcid", "my-fake-username")
	assert.Nil(err, "Unable to set KBase local username for ORCID!")

	// doing the same thing again should be fine
	err = SetKBaseLocalUsernameForOrcid("my-fake-orcid", "my-fake-username")
	assert.Nil(err, "Setting KBase local username is not idempotent!")
}

// tests whether inconsistently reset ORCID/user registrations trigger errors
func TestResetKBaseLocalUsernameForOrcid(t *testing.T) {
	assert := assert.New(t)
	err := SetKBaseLocalUsernameForOrcid("my-fake-orcid", "my-fake-username")
	err = SetKBaseLocalUsernameForOrcid("my-fake-orcid", "your-fake-username")
	assert.NotNil(err)
}

// tests whether the authentication server can return the username for the
// for the owner of the developer token
func TestKBaseLocalUsername(t *testing.T) {
	assert := assert.New(t)
	err := SetKBaseLocalUsernameForOrcid("my-fake-orcid", "my-fake-username")
	username, err := KBaseLocalUsernameForOrcid("my-fake-orcid")
	assert.Nil(err)
	assert.Equal("my-fake-username", username)
}

// tests for fetching a kbase username for an unregistered ORCID
func TestUnregisteredKBaseLocalUsername(t *testing.T) {
	assert := assert.New(t)
	err := SetKBaseLocalUsernameForOrcid("my-fake-orcid", "my-fake-username")
	username, err := KBaseLocalUsernameForOrcid("your-fake-orcid")
	assert.NotNil(err)
	assert.Equal("", username)
}
