package config

// These tests verify that we can properly configure the search service with
// YAML input.
import (
	"log"
	"os"

	"github.com/stretchr/testify/assert"
	"testing"
)

// The working directory from which the tests were invoked.
var CWD string

// The temporary testing directory
var TESTING_DIR string

// Tests whether config.Init reports an error for blank input.
func TestInitRejectsBlankInput(t *testing.T) {
	yaml := ""
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Blank config didn't trigger an error.")
}

// Tests whether config.Init reports an error for an invalid max number of
// processes.
func TestInitRejectsBadPort(t *testing.T) {
	yaml := "service:\n  port: -1\n\n"
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with bad port didn't trigger an error.")
	yaml = "service:\n  port: 1000000\n\n" +
		"stores:\n  - progreb_store.db"
	b = []byte(yaml)
	err = Init(b)
	assert.NotNil(t, err, "Config with bad port didn't trigger an error.")
}

// Tests whether config.Init reports an error for an invalid max number of
// connections.
func TestInitRejectsBadMaxConnections(t *testing.T) {
	yaml := "service:\n  maxConnections: 0\n\n" +
		"stores:\n  - progreb_store.db"
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with bad maxConnections didn't trigger an error.")
}

// Tests whether config.Init rejects a configuration with no databases.
func TestInitRejectsNoDatabasesDefined(t *testing.T) {
	yaml := fmt.Sprintf("engines:\n  last:\n    executable: \"%s\"\n\n",
		LAST_SURROGATE)
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
	yaml := "service:\n\n" +
		"stores:\n  - pogreb_store.db\n  - pogreb_store.db\n"
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with duplicate data store didn't trigger "+
		"an error.")
}

// Tests whether config.Init reports an error for duplicate databases
func TestInitRejectsDuplicateDatabases(t *testing.T) {
	yaml := "service:\n\n" +
		"stores:\n  - pogreb_store.db\n  - pogreb_store.db\n"
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with duplicate data store didn't trigger "+
		"an error.")
}

// Tests whether config.Init returns no error for a configuration that is
// (ostensibly) valid. NOTE: This particular configuration is consistent and
// contains acceptible values for fields. It won't actually run a service!
func TestInitAcceptsValidInput(t *testing.T) {
	yaml := "service:\n  port: 8080\n  maxQueries: 50\n" +
		"  maxConnections: 50\n\n" +
		"stores:\n  - pogreb_store.db\n\n"
	b := []byte(yaml)
	err := Init(b)
	assert.Nil(t, err, "Valid YAML input produced an error: %s", err)
}

// Tests whether config.Init properly initializes its globals for valid input.
func TestInitProperlySetsGlobals(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	yaml := "service:\n  port: 8080\n  maxConnections: 50\n\n" +
		"stores:\n  - pogreb_store.db\n\n"
	b := []byte(yaml)
	err := Init(b)
	assert.Nil(err, "Valid YAML input produced an error: %s", err)

	// Check data
	assert.Equal(8080, Service.Port)
	assert.Equal(50, Service.MaxConnections)
	assert.Equal(1, len(Stores))
	assert.Equal("pogreb_store.db", Stores[0])
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
	TESTING_DIR, err = os.MkdirTemp(os.TempDir(), "seqidmap-config-tests-")
	if err != nil {
		log.Panicf("Couldn't create testing directory: %s", err)
	}

	// Change to the testing directory and write a phony data store.
	os.Chdir(TESTING_DIR)
	os.Mkdir("pogreb_store.db", 0644)
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
