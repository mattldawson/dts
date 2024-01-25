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

// These tests must be run serially, since tasks are coordinated by a
// single instance.

package tasks

import (
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/core"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/dtstest"
	"github.com/kbase/dts/endpoints"
)

// temporary testing directory
var TESTING_DIR string

// a directory in which the task manager can read/write files
var dataDirectory string

// the amount of time it takes a test database to stage files
var stagingDuration time.Duration = time.Duration(150) * time.Millisecond

// the amount of time it takes a test endpoint to transfer files
var transferDuration time.Duration = time.Duration(500) * time.Millisecond

// a pause to give the task manager a bit of time
var pause time.Duration = time.Duration(25) * time.Millisecond

// configuration
const tasksConfig string = `
service:
  port: 8080
  max_connections: 100
  poll_interval: 50  # milliseconds
  data_dir: TESTING_DIR/data
  delete_after: 2    # seconds
  endpoint: local-endpoint
databases:
  source:
    name: Source Test Database
    organization: The Source Company
    endpoint: source-endpoint
  destination:
    name: Destination Test Database
    organization: Fabulous Destinations, Inc.
    endpoint: destination-endpoint
endpoints:
  local-endpoint:
    name: Local endpoint
    id: 8816ec2d-4a48-4ded-b68a-5ab46a4417b6
    provider: fake
    root: TESTING_DIR
  source-endpoint:
    name: Endpoint 1
    id: 26d61236-39f6-4742-a374-8ec709347f2f
    provider: fake
    root: SOURCE_ROOT
  destination-endpoint:
    name: Endpoint 2
    id: f1865b86-2c64-4b8b-99f3-5aaa945ec3d9
    provider: fake
    root: DESTINATION_ROOT
`

// file test metadata
var testResources map[string]DataResource = map[string]DataResource{
	"file1": DataResource{
		Id:     "file1",
		Name:   "file1.dat",
		Path:   "dir1/file1.dat",
		Format: "text",
		Bytes:  1024,
		Hash:   "d91f97974d06563cab48d4d43a17e08a",
	},
	"file2": DataResource{
		Id:     "file2",
		Name:   "file2.dat",
		Path:   "dir2/file2.dat",
		Format: "text",
		Bytes:  2048,
		Hash:   "d91f9e974d0e563cab48d4d43a17e08a",
	},
	"file3": DataResource{
		Id:     "file3",
		Name:   "file3.dat",
		Path:   "dir3/file3.dat",
		Format: "text",
		Bytes:  4096,
		Hash:   "e91f9e974d0e563cab48d4d43a17e08e",
	},
}

// this function gets called at the begіnning of a test session
func setup() {
	dtstest.EnableDebugLogging()

	log.Print("Creating testing directory...\n")
	var err error
	TESTING_DIR, err = os.MkdirTemp(os.TempDir(), "data-transfer-service-tests-")
	if err != nil {
		log.Panicf("Couldn't create testing directory: %s", err)
	}
	os.Chdir(TESTING_DIR)

	// register test databases/endpoints referred to in config file
	databases.RegisterDatabase("source", NewFakeSourceDatabase)
	databases.RegisterDatabase("destination", NewFakeDestinationDatabase)
	endpoints.RegisterEndpointProvider("fake", NewFakeEndpoint)

	// read in the config file with SOURCE_ROOT and DESTINATION_ROOT replaced
	myConfig := strings.ReplaceAll(tasksConfig, "TESTING_DIR", TESTING_DIR)
	err = config.Init([]byte(myConfig))
	if err != nil {
		log.Panicf("Couldn't initialize configuration: %s", err)
	}

	// Create the data directory used to save/restore tasks
	os.Mkdir(config.Service.DataDirectory, 0755)
}

// this function gets called after all tests have been run
func breakdown() {
	if TESTING_DIR != "" {
		log.Printf("Deleting testing directory %s...\n", TESTING_DIR)
		os.RemoveAll(TESTING_DIR)
	}
}

// To run the tests serially, we attach them to a SerialTests type and
// have them run by a a single test runner.
type SerialTests struct{ Test *testing.T }

func (t *SerialTests) TestStartAndStop() {
	assert := assert.New(t.Test)

	assert.False(Running())
	err := Start()
	assert.Nil(err)
	assert.True(Running())
	err = Stop()
	assert.Nil(err)
	assert.False(Running())
}

func (t *SerialTests) TestCreateTask() {
	assert := assert.New(t.Test)

	err := Start()
	assert.Nil(err)

	pollInterval := time.Duration(config.Service.PollInterval) * time.Millisecond
	deleteAfter := time.Duration(config.Service.DeleteAfter) * time.Second

	// queue up a transfer task between two phony databases
	orcid := "1234-5678-9012-3456"
	taskId, err := Create(orcid, "source", "destination", []string{"file1", "file2"})
	assert.Nil(err)
	assert.True(taskId != uuid.UUID{})

	// the initial status of the task should be Unknown
	status, err := Status(taskId)
	assert.Nil(err)
	assert.Equal(TransferStatusUnknown, status.Code)

	// make sure the status switches to staging
	time.Sleep(pause + pollInterval)
	status, err = Status(taskId)
	assert.Nil(err)
	assert.Equal(TransferStatusStaging, status.Code)

	// wait for the staging to complete and then check its status
	// again (should be actively transferring)
	time.Sleep(pause + stagingDuration)
	status, err = Status(taskId)
	assert.Nil(err)
	assert.Equal(TransferStatusActive, status.Code)

	// wait again for the transfer to complete and then check its status
	// (should be finalizing or have successfully completed)
	time.Sleep(pause + transferDuration)
	status, err = Status(taskId)
	assert.Nil(err)
	assert.True(status.Code == TransferStatusFinalizing || status.Code == TransferStatusSucceeded)

	// if the transfer was finalizing, check once more for completion
	if status.Code != TransferStatusSucceeded {
		time.Sleep(pause + transferDuration)
		status, err = Status(taskId)
		assert.Nil(err)
		assert.Equal(TransferStatusSucceeded, status.Code)
	}

	// now wait for the task to age out and make sure it's not found
	time.Sleep(pause + deleteAfter)
	status, err = Status(taskId)
	assert.NotNil(err)

	err = Stop()
	assert.Nil(err)
}

func (t *SerialTests) TestCancelTask() {
	assert := assert.New(t.Test)

	err := Start()
	assert.Nil(err)

	pollInterval := time.Duration(config.Service.PollInterval) * time.Millisecond

	// queue up a transfer task between two phony databases
	orcid := "1234-5678-9012-3456"
	taskId, err := Create(orcid, "source", "destination", []string{"file1", "file2"})
	assert.Nil(err)
	assert.True(taskId != uuid.UUID{})

	// get things going and make sure we can check its status
	time.Sleep(pause + pollInterval)
	_, err = Status(taskId)
	assert.Nil(err)

	// cancel the thing
	err = Cancel(taskId)
	assert.Nil(err)

	// wait for the task to complete
	status, err := Status(taskId)
	for {
		if status.Code == TransferStatusSucceeded ||
			status.Code == TransferStatusFailed {
			break
		}
		time.Sleep(pause + pollInterval)
		status, err = Status(taskId)
		assert.Nil(err)
	}

	err = Stop()
	assert.Nil(err)
}

func (t *SerialTests) TestStopAndRestart() {
	assert := assert.New(t.Test)

	// start up, add a bunch of tasks, then immediately close
	err := Start()
	assert.Nil(err)
	orcid := "1234-5678-9012-3456"
	numTasks := 10
	taskIds := make([]uuid.UUID, numTasks)
	for i := 0; i < numTasks; i++ {
		taskId, _ := Create(orcid, "source", "destination", []string{"file1", "file2"})
		taskIds[i] = taskId
	}
	time.Sleep(100 * time.Millisecond) // let things settle
	err = Stop()
	assert.Nil(err)

	// now restart the task manager and make sure all the tasks are there
	err = Start()
	assert.Nil(err)
	for i := 0; i < numTasks; i++ {
		_, err := Status(taskIds[i])
		assert.Nil(err)
	}

	err = Stop()
	assert.Nil(err)
}

// runs all the serial tests... serially!
func TestRunner(t *testing.T) {
	tester := SerialTests{Test: t}
	tester.TestStartAndStop()
	tester.TestCreateTask()
	tester.TestCancelTask()
	tester.TestStopAndRestart()
}

// This runs setup, runs all tests, and does breakdown.
func TestMain(m *testing.M) {
	var status int
	setup()
	status = m.Run()
	breakdown()
	os.Exit(status)
}

//-------------------
// Testing Apparatus
//-------------------

type FakeStagingRequest struct {
	FileIds []string
	Time    time.Time
}

// This type implements core.Database with only enough behavior
// to test the task manager.
type FakeDatabase struct {
	Endpt   core.Endpoint
	Staging map[uuid.UUID]FakeStagingRequest
}

// creates a new fake database that stages its 3 files
func NewFakeSourceDatabase(orcid string) (core.Database, error) {
	endpoint, err := endpoints.NewEndpoint(config.Databases["source"].Endpoint)
	if err != nil {
		return nil, err
	}
	db := FakeDatabase{
		Endpt:   endpoint,
		Staging: make(map[uuid.UUID]FakeStagingRequest),
	}
	db.Endpt.(*FakeEndpoint).Database = &db
	return &db, nil
}

// ... and a fake destination database that doesn't have to do anything
func NewFakeDestinationDatabase(orcid string) (core.Database, error) {
	endpoint, err := endpoints.NewEndpoint(config.Databases["destination"].Endpoint)
	if err != nil {
		return nil, err
	}
	db := FakeDatabase{
		Endpt: endpoint,
	}
	db.Endpt.(*FakeEndpoint).Database = &db
	return &db, nil
}

func (db *FakeDatabase) Search(params core.SearchParameters) (core.SearchResults, error) {
	// this method is unused, so we just need a placeholder
	return core.SearchResults{}, nil
}

func (db *FakeDatabase) Resources(fileIds []string) ([]core.DataResource, error) {
	resources := make([]core.DataResource, 0)
	var err error
	for _, fileId := range fileIds {
		if resource, found := testResources[fileId]; found {
			resources = append(resources, resource)
		} else {
			err = fmt.Errorf("Unrecognized File ID: %s", fileId)
			break
		}
	}
	return resources, err
}

func (db *FakeDatabase) StageFiles(fileIds []string) (uuid.UUID, error) {
	id := uuid.New()
	db.Staging[id] = FakeStagingRequest{
		FileIds: fileIds,
		Time:    time.Now(),
	}
	return id, nil
}

func (db *FakeDatabase) StagingStatus(id uuid.UUID) (core.StagingStatus, error) {
	if info, found := db.Staging[id]; found {
		if time.Now().Sub(info.Time) >= stagingDuration {
			return core.StagingStatusSucceeded, nil
		}
		return core.StagingStatusActive, nil
	} else {
		return core.StagingStatusUnknown, nil
	}
}

func (db *FakeDatabase) Endpoint() (Endpoint, error) {
	return db.Endpt, nil
}

func (db *FakeDatabase) LocalUser(orcid string) (string, error) {
	return "fakeuser", nil
}

type TransferInfo struct {
	Time   time.Time // transfer initiation time
	Status TransferStatus
}

// This type implements core.Endpoint with only enough behavior
// to test the task manager.
type FakeEndpoint struct {
	Database         *FakeDatabase // fake database attached to endpoint
	TransferDuration time.Duration // time it takes to "transfer files"
	Xfers            map[uuid.UUID]TransferInfo
}

// creates a new fake source endpoint that transfers a fictional payload in 1
// second
func NewFakeEndpoint(name string) (core.Endpoint, error) {
	return &FakeEndpoint{
		TransferDuration: transferDuration,
		Xfers:            make(map[uuid.UUID]TransferInfo),
	}, nil
}

func (ep *FakeEndpoint) Root() string {
	root, _ := os.Getwd()
	return root
}

func (ep *FakeEndpoint) FilesStaged(files []DataResource) (bool, error) {
	if ep.Database != nil {
		// are there any unrecognized files?
		for _, file := range files {
			if _, found := testResources[file.Id]; !found {
				return false, fmt.Errorf("Unrecognized file: %s\n", file.Id)
			}
		}
		// the source endpoint should report true for the staged files as long
		// as the source database has had time to stage them
		for _, req := range ep.Database.Staging {
			if time.Now().Sub(req.Time) < stagingDuration {
				return false, nil
			}
		}
	}
	return true, nil
}

func (ep *FakeEndpoint) Transfers() ([]uuid.UUID, error) {
	xfers := make([]uuid.UUID, 0)
	for xferId, _ := range ep.Xfers {
		xfers = append(xfers, xferId)
	}
	return xfers, nil
}

func (ep *FakeEndpoint) Transfer(dst Endpoint, files []FileTransfer) (uuid.UUID, error) {
	xferId := uuid.New()
	ep.Xfers[xferId] = TransferInfo{
		Time: time.Now(),
		Status: TransferStatus{
			Code:                TransferStatusActive,
			NumFiles:            len(files),
			NumFilesTransferred: 0,
		},
	}
	return xferId, nil
}

func (ep *FakeEndpoint) Status(id uuid.UUID) (TransferStatus, error) {
	if info, found := ep.Xfers[id]; found {
		if info.Status.Code != TransferStatusSucceeded && time.Now().Sub(info.Time) >= transferDuration { // update if needed
			info.Status.Code = TransferStatusSucceeded
			ep.Xfers[id] = info
		}
		return info.Status, nil
	} else {
		return TransferStatus{}, fmt.Errorf("Invalid transfer ID: %s", id.String())
	}
}

func (ep *FakeEndpoint) Cancel(id uuid.UUID) error {
	// not used (yet)
	return nil
}
