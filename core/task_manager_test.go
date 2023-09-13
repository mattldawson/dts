package core

import (
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

// the interval at which our test task manager polls to update status
var pollInterval time.Duration = time.Duration(50) * time.Millisecond

// the amount of time it takes a test database to stage files
var stagingDuration time.Duration = time.Duration(150) * time.Millisecond

// the amount of time it takes a test endpoint to transfer files
var transferDuration time.Duration = time.Duration(500) * time.Millisecond

// a pause to give the task manager a bit of time
var pause time.Duration = time.Duration(25) * time.Millisecond

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
}

// this function gets called after all tests have been run
func breakdown() {
}

func TestNewTaskManager(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	mgr, err := NewTaskManager(pollInterval)
	assert.NotNil(mgr)
	assert.Nil(err)
	assert.Equal(pollInterval, mgr.PollInterval)

	mgr.Close()
}

func TestAddTask(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	mgr, err := NewTaskManager(pollInterval)
	assert.Nil(err)

	// queue up a transfer task between two phony databases
	src := NewFakeDatabase()
	dest := NewFakeDatabase()
	taskId, err := mgr.Add(src, dest, []string{"file1", "file2"})
	assert.Nil(err)
	assert.True(taskId != uuid.UUID{})

	// check its status (should be staging)
	status, err := mgr.Status(taskId)
	assert.Nil(err)
	assert.Equal(TransferStatusStaging, status.Code)

	// wait for the staging to complete and then check its status
	// again (should be actively transferring)
	time.Sleep(pause + stagingDuration)
	status, err = mgr.Status(taskId)
	assert.Nil(err)
	assert.Equal(TransferStatusActive, status.Code)

	// wait again for the transfer to complete and then check its status
	// (should have successfully completed)
	time.Sleep(pause + transferDuration)
	status, err = mgr.Status(taskId)
	assert.Nil(err)
	assert.Equal(TransferStatusSucceeded, status.Code)

	mgr.Close()
}

// This runs setup, runs all tests, and does breakdown.
func TestMain(m *testing.M) {
	var status int
	setup()
	status = m.Run()
	breakdown()
	os.Exit(status)
}

type FakeStagingRequest struct {
	FileIds []string
	Time    time.Time
}

// This type implements core.Database with only enough behavior
// to test the task manager.
type FakeDatabase struct {
	Endpt   *FakeEndpoint
	Staging map[uuid.UUID]FakeStagingRequest
}

// creates a new fake database that stages its 3 files
func NewFakeDatabase() *FakeDatabase {
	db := FakeDatabase{
		Endpt:   NewFakeEndpoint(),
		Staging: make(map[uuid.UUID]FakeStagingRequest),
	}
	db.Endpt.Database = &db
	return &db
}

func (db *FakeDatabase) Search(params SearchParameters) (SearchResults, error) {
	// this method is unused, so we just need a placeholder
	return SearchResults{}, nil
}

func (db *FakeDatabase) Resources(fileIds []string) ([]DataResource, error) {
	resources := make([]DataResource, 0)
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

func (db *FakeDatabase) StagingStatus(id uuid.UUID) (StagingStatus, error) {
	if info, found := db.Staging[id]; found {
		if time.Now().Sub(info.Time) >= stagingDuration {
			return StagingStatusSucceeded, nil
		} else {
			return StagingStatusActive, nil
		}
	} else {
		return StagingStatusUnknown, nil
	}
}

func (db *FakeDatabase) Endpoint() Endpoint {
	return db.Endpt
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
func NewFakeEndpoint() *FakeEndpoint {
	return &FakeEndpoint{
		TransferDuration: transferDuration,
		Xfers:            make(map[uuid.UUID]TransferInfo),
	}
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
