package databases

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewJDPDatabase(t *testing.T) {
	assert := assert.New(t) // binds assert to t
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	jdpDb, err := NewDatabase(orcid, "jdp")
	assert.NotNil(jdpDb, "JDP database not created")
	assert.Nil(err, "JDP database creation encountered an error")
	jdpDb.Close()
}

func TestInvalidDatabase(t *testing.T) {
	assert := assert.New(t) // binds assert to t
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	bbDb, err := NewDatabase(orcid, "booga booga")
	assert.Nil(bbDb, "Invalid database should not be created")
	assert.NotNil(err, "Invalid database creation did not report an error")
}
