package kbase

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/dtstest"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/endpoints/globus"
)

const kbaseConfig string = `
databases:
  kbase:
    name: KBase Workspace Service (KSS)
    organization: KBase
    endpoint: globus-kbase
endpoints:
  globus-kbase:
    name: KBase
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
`

// this function gets called at the begіnning of a test session
func setup() {
	dtstest.EnableDebugLogging()
	config.Init([]byte(kbaseConfig))
	databases.RegisterDatabase("kbase", NewDatabase)
	endpoints.RegisterEndpointProvider("globus", globus.NewEndpoint)
}

// this function gets called after all tests have been run
func breakdown() {
}

func TestNewDatabase(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, err := NewDatabase(orcid)
	assert.NotNil(db, "KBase database not created")
	assert.Nil(err, "KBase database creation encountered an error")
}

func TestNewDatabaseWithoutOrcid(t *testing.T) {
	assert := assert.New(t)
	db, err := NewDatabase("")
	assert.Nil(db, "Invalid KBase database somehow created")
	assert.NotNil(err, "KBase database creation without ORCID encountered no error")
}

func TestSearch(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase(orcid)
	params := databases.SearchParameters{
		Query: "prochlorococcus",
		Pagination: struct {
			Offset, MaxNum int
		}{
			Offset: 1,
			MaxNum: 50,
		},
	}
	_, err := db.Search(params)
	assert.NotNil(err, "Search not implemented for kbase database!")
}

func TestResources(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase(orcid)
	_, err := db.Resources(nil)
	assert.NotNil(err, "Resources not implemented for kbase database!")
}

func TestLocalUser(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase(orcid)
	username, err := db.LocalUser(orcid)
	assert.Nil(err)
	assert.True(len(username) > 0)
}

// this runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	setup()
	status := m.Run()
	breakdown()
	os.Exit(status)
}
