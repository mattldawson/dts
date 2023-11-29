package jdp

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"dts/config"
	"dts/core"
)

const jdpConfig string = `
databases:
  jdp:
    name: JGI Data Portal
    organization: Joint Genome Institue
    url: https://files.jgi.doe.gov
    endpoint: globus-jdp
    auth:
      client_id: ${JGI_CLIENT_ID}
      client_secret: ${JGI_CLIENT_SECRET}
endpoints:
  globus-jdp:
    name: Globus NERSC DTN
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
`

// this function gets called at the begіnning of a test session
func setup() {
	config.Init([]byte(jdpConfig))
}

// this function gets called after all tests have been run
func breakdown() {
}

func TestNewDatabase(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	jdpDb, err := NewDatabase(orcid)
	assert.NotNil(jdpDb, "JDP database not created")
	assert.Nil(err, "JDP database creation encountered an error")
}

func TestNewDatabaseWithoutOrcid(t *testing.T) {
	assert := assert.New(t)
	jdpDb, err := NewDatabase("")
	assert.Nil(jdpDb, "Invalid JDP database somehow created")
	assert.NotNil(err, "JDP database creation without ORCID encountered no error")
}

func TestNewDatabaseWithoutJDPSharedSecret(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	jdpSecret := os.Getenv("DTS_JDP_SECRET")
	os.Unsetenv("DTS_JDP_SECRET")
	jdpDb, err := NewDatabase(orcid)
	os.Setenv("DTS_JDP_SECRET", jdpSecret)
	assert.Nil(jdpDb, "JDP database somehow created without shared secret available")
	assert.NotNil(err, "JDP database creation without shared secret encountered no error")
}

func TestSearch(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase(orcid)
	params := core.SearchParameters{
		Query: "prochlorococcus",
		Pagination: struct {
			Offset, MaxNum int
		}{
			Offset: 1,
			MaxNum: 50,
		},
	}
	results, err := db.Search(params)
	assert.True(len(results.Resources) > 0, "JDP search query returned no results")
	assert.Nil(err, "JDP search query encountered an error")
}

func TestResources(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase(orcid)
	params := core.SearchParameters{
		Query: "prochlorococcus",
	}
	results, _ := db.Search(params)
	fileIds := make([]string, len(results.Resources))
	for i, res := range results.Resources {
		fileIds[i] = res.Id
	}
	resources, err := db.Resources(fileIds[:10])
	assert.Nil(err, "JDP resource query encountered an error")
	assert.Equal(10, len(resources),
		"JDP resource query didn't return requested number of results")
	// JAMO doesn't return source/credit metadata, and sometimes doesn't
	// have hashes either, so we have to check field by field
	for i, _ := range resources {
		jdpSearchResult := results.Resources[i]
		resource := resources[i]
		assert.Equal(jdpSearchResult.Id, resource.Id, "Resource ID mismatch")
		assert.Equal(jdpSearchResult.Name, resource.Name, "Resource name mismatch")
		assert.Equal(jdpSearchResult.Path, resource.Path, "Resource path mismatch")
		assert.Equal(jdpSearchResult.Format, resource.Format, "Resource format mismatch")
		assert.Equal(jdpSearchResult.Bytes, resource.Bytes, "Resource size mismatch")
		assert.Equal(jdpSearchResult.MediaType, resource.MediaType, "Resource media type mismatch")
		assert.Equal(jdpSearchResult.Credit.Identifier, resource.Credit.Identifier, "Resource credit ID mismatch")
		assert.Equal(jdpSearchResult.Credit.ResourceType, resource.Credit.ResourceType, "Resource credit resource type mismatch")
	}
}

func TestEndpoint(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase(orcid)
	endpoint := db.Endpoint()
	assert.NotNil(endpoint, "JDP database has no endpoint")
}

// this runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	var status int
	setup()
	status = m.Run()
	breakdown()
	os.Exit(status)
}
