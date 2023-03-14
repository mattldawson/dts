package config

// These tests verify that we can properly configure the search service with
// YAML input.
import (
	"log"
	"os"

	"github.com/stretchr/testify/assert"
	"testing"
)

// the working directory from which the tests were invoked.
var CWD string

// the temporary testing directory
var TESTING_DIR string

// a valid service config entry
const VALID_SERVICE string = `
service:
  port: 8080
  maxConnections: 100

`

// a valid endpoints config entry
const VALID_ENDPOINTS string = `
endpoints:
  globus:
    globus-jdp:
      user: dts@example.com
      url: dtn1.nersc.gov

`

// a valid single database config entry
const VALID_DATABASE string = `
`

// a valid databases config entry
const VALID_DATABASES string = `
databases:
  jdp:
    name: JGI Data Portal
    organization: Joint Genome Institute
    url: files.jgi.doe.gov
    endpoint: globus-jdp # local file transfer endpoint
    notifications: jgi-mq
    search: # how to search for files (GET)
      elasticsearch: # elasticsearch method
        resource: /search
        query_parameter: q # any other interesting ES-related parameters?
      # other kinds of search supported?
    transfer: # how to initiate a transfer (POST)
      resource: /download_files
      request: # fields in request body
        globus_user_name: endpoints.globus.globus-jdp.user

`

// tests whether config.Init reports an error for blank input
func TestInitRejectsBlankInput(t *testing.T) {
	yaml := ""
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Blank config didn't trigger an error.")
}

// tests whether config.Init reports an error for an invalid max number of
// processes
func TestInitRejectsBadPort(t *testing.T) {
	yaml := "service:\n  port: -1\n\n" + VALID_ENDPOINTS + VALID_DATABASES
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with bad port didn't trigger an error.")
	yaml = "service:\n  port: 1000000\n\n" + VALID_ENDPOINTS + VALID_DATABASES
	b = []byte(yaml)
	err = Init(b)
	assert.NotNil(t, err, "Config with bad port didn't trigger an error.")
}

// tests whether config.Init reports an error for an invalid max number of
// connections
func TestInitRejectsBadMaxConnections(t *testing.T) {
	yaml := "service:\n  maxConnections: 0\n\n" + VALID_ENDPOINTS + VALID_DATABASES
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with bad maxConnections didn't trigger an error.")
}

// tests whether config.Init rejects a configuration with no endpoints
func TestInitRejectsNoEndpointsDefined(t *testing.T) {
	yaml := VALID_SERVICE + VALID_DATABASES
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with no endpoints didn't trigger an error.")
}

// tests whether config.Init rejects a configuration with no endpoints
func TestInitRejectsInvalidEndpointType(t *testing.T) {
	yaml := VALID_SERVICE + VALID_DATABASES +
		"endpoints:\n  eeeevil_endpoint_type:\n    eeeevil_field: eeeevil_value\n\n"
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with invalid endpoint didn't trigger an error.")
}

// tests whether config.Init rejects a configuration with no databases
func TestInitRejectsNoDatabasesDefined(t *testing.T) {
	yaml := VALID_SERVICE + VALID_ENDPOINTS
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with no databases didn't trigger an error.")
}

// Tests whether config.Init rejects a database with a bad base URL.
func TestInitRejectsBadDatabaseBaseURL(t *testing.T) {
	yaml := fmt.Sprintf("engines:\n  last:\n    executable: \"%s\"\n\n",
		LAST_SURROGATE) +
		"databases:\n  FakeDB:\n    engine: last\n" +
		"    dir: /bad/database/dir\n\n"
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with bad database dir didn't trigger an error.")
}

// Tests whether config.Init rejects a database with a query template without
// a token for inserting ElasticSearch queries
func TestInitRejectsBadDatabaseESQueryTemplateAndToken(t *testing.T) {
	yaml := VALID_SERVICE + VALID_ENDPOINTS
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with duplicate data store didn't trigger "+
		"an error.")
}

// Tests whether config.Init returns no error for a configuration that is
// (ostensibly) valid. NOTE: This particular configuration is consistent and
// contains acceptible values for fields. It won't actually run a service!
func TestInitAcceptsValidInput(t *testing.T) {
	yaml := VALID_SERVICE + VALID_ENDPOINTS + VALID_DATABASES
	b := []byte(yaml)
	err := Init(b)
	assert.Nil(t, err, "Valid YAML input produced an error: %s", err)
}

// Tests whether config.Init properly initializes its globals for valid input.
func TestInitProperlySetsGlobals(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	yaml := VALID_SERVICE + VALID_ENDPOINTS + VALID_DATABASES
	b := []byte(yaml)
	err := Init(b)
	assert.Nil(err, "Valid YAML input produced an error: %s", err)

	// Check data
	assert.Equal(8080, Service.Port)
	assert.Equal(100, Service.MaxConnections)
	assert.Equal(1, len(Endpoints))
	assert.Equal(1, len(Databases))
}

// Performs testing setup.
func setup() {
	// Jot down our CWD, create a temporary directory, and change to it.
	var err error
	CWD, err = os.Getwd()
	if err != nil {
		log.Panicf("Couldn't get current working directory: %s", err)
	}
	log.Print("Creating testing directory...\n")
	TESTING_DIR, err = os.MkdirTemp(os.TempDir(), "dts-config-tests-")
	if err != nil {
		log.Panicf("Couldn't create testing directory: %s", err)
	}
}

// Performs testing breakdown.
func breakdown() {
	// Change back to our original CWD.
	os.Chdir(CWD)

	if TESTING_DIR != "" {
		// Remove the testing directory and its contents.
		log.Printf("Deleting testing directory %s...\n", TESTING_DIR)
		os.RemoveAll(TESTING_DIR)
	}
}

// This runs setup, runs all tests, and does breakdown.
func TestMain(m *testing.M) {
	var status int
	setup()
	if TESTING_DIR != "" {
		status = m.Run()
	}
	breakdown()
	os.Exit(status)
}
