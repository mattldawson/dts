package databases

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInvalidDatabase(t *testing.T) {
	assert := assert.New(t)
	bbDb, err := NewDatabase("booga booga")
	assert.Nil(bbDb, "Invalid database should not be created")
	assert.NotNil(err, "Invalid database creation did not report an error")
}
