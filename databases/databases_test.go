package databases

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInvalidDatabase(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	bbDb, err := NewDatabase(orcid, "booga booga")
	assert.Nil(bbDb, "Invalid database should not be created")
	assert.NotNil(err, "Invalid database creation did not report an error")
}
