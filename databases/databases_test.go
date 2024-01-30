package databases

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// this function gets called at the begіnning of a test session
func setup() {
	RegisterDatabase("jdp", jdp.NewDatabase)
}

// this function gets called after all tests have been run
func breakdown() {
}

func TestInvalidDatabase(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	bbDb, err := NewDatabase(orcid, "booga booga")
	assert.Nil(bbDb, "Invalid database should not be created")
	assert.NotNil(err, "Invalid database creation did not report an error")
}
