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

package globus

import (
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"dts/config"
	"dts/core"
)

// source database files by ID
// (these files exist on Globus Tutorial Endpoint 1 (see below) for exactly
// this sort of testing)
var sourceFilesById = map[string]string{
	"1": "share/godata/file1.txt",
	"2": "share/godata/file2.txt",
	"3": "share/godata/file3.txt",
}
var destinationFilesById = map[string]string{
	"1": "dts-test-dir/file1.txt",
	"2": "dts-test-dir/file2.txt",
	"3": "dts-test-dir/file3.txt",
}

const globusConfig string = `
endpoints:
  source:
    name: Globus Tutorial Endpoint 1
    id: ddb59aef-6d04-11e5-ba46-22000b92c6ec
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
  destination:
    name: Globus Tutorial Endpoint 2
    id: ddb59af0-6d04-11e5-ba46-22000b92c6ec
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
  not-globus-jdp:
    name: NERSC DTN
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: not-globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
`

// this function gets called at the begіnning of a test session
func setup() {
	config.Init([]byte(globusConfig))
}

// this function gets called after all tests have been run
func breakdown() {
}

func TestGlobusConstructor(t *testing.T) {
	assert := assert.New(t)

	endpoint, err := NewEndpoint("source")
	assert.NotNil(endpoint)
	assert.Nil(err)
}

func TestBadGlobusConstructor(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	endpoint, err := NewEndpoint("not-globus-jdp")
	assert.Nil(endpoint)
	assert.NotNil(err)
}

func TestGlobusTransfers(t *testing.T) {
	assert := assert.New(t)
	endpoint, _ := NewEndpoint("source")
	// this is just a smoke test--we don't check the contents of the result
	xfers, err := endpoint.Transfers()
	assert.NotNil(xfers) // empty or non-empty slice
	assert.Nil(err)
}

func TestGlobusFilesStaged(t *testing.T) {
	assert := assert.New(t)
	endpoint, _ := NewEndpoint("source")

	// provide an empty slice of filenames, which should return true
	staged, err := endpoint.FilesStaged([]core.DataResource{})
	assert.True(staged)
	assert.Nil(err)

	// provide a file that's known to be on the source endpoint, which
	// should return true
	resources := make([]core.DataResource, 0)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("%d", i)
		resources = append(resources, core.DataResource{
			Id:   id,
			Path: sourceFilesById[id],
		})
	}
	staged, err = endpoint.FilesStaged(resources)
	assert.True(staged)
	assert.Nil(err)

	// provide a nonexistent file, which should return false
	resources = []core.DataResource{
		core.DataResource{
			Id:   "yadda",
			Path: "yaddayadda/yadda/yaddayadda/yaddayaddayadda.xml",
		},
	}
	staged, err = endpoint.FilesStaged(resources)
	assert.False(staged)
	assert.Nil(err)
}

func TestGlobusTransfer(t *testing.T) {
	assert := assert.New(t)
	source, _ := NewEndpoint("source")
	destination, _ := NewEndpoint("destination")

	fileXfers := make([]core.FileTransfer, 0)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("%d", i)
		fileXfers = append(fileXfers, core.FileTransfer{
			SourcePath:      sourceFilesById[id],
			DestinationPath: destinationFilesById[id],
		})
	}
	_, err := source.Transfer(destination, fileXfers)
	assert.Nil(err)
}

func TestBadGlobusTransfer(t *testing.T) {
	assert := assert.New(t)
	source, _ := NewEndpoint("source")
	destination, _ := NewEndpoint("destination")

	// ask for some nonexistent files
	fileXfers := make([]core.FileTransfer, 0)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("%d", i)
		fileXfers = append(fileXfers, core.FileTransfer{
			SourcePath:      sourceFilesById[id] + "_with_bad_suffix",
			DestinationPath: destinationFilesById[id],
		})
	}
	_, err := source.Transfer(destination, fileXfers)
	assert.NotNil(err)
}

func TestUnknownGlobusStatus(t *testing.T) {
	assert := assert.New(t)
	endpoint, _ := NewEndpoint("source")

	// make up a bogus transfer UUID and check its status
	taskId := uuid.New()
	status, err := endpoint.Status(taskId)
	assert.Equal(core.TransferStatusUnknown, status.Code)
	assert.NotNil(err)
}

// this runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	var status int
	setup()
	status = m.Run()
	breakdown()
	os.Exit(status)
}
