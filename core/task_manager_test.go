package core

import (
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

// this function gets called at the begіnning of a test session
func setup() {
}

// this function gets called after all tests have been run
func breakdown() {
}

func TestNewTaskManager(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	mgr, err := NewTaskManager(time.Duration(100) * time.Millisecond)
	assert.NotNil(mgr)
	assert.Nil(err)
	assert.Equal(mgr.PollInterval, time.Duration(100)*time.Millisecond)

	mgr.Close()
}

func TestAddTask(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	mgr, err := NewTaskManager(time.Duration(100) * time.Millisecond)
	assert.Nil(err)
	src := NewFakeDatabase()
	dest := NewFakeDatabase()
	taskId, err := mgr.Add(src, dest, []string{"file_a.dat", "file_b.dat"})
	assert.Nil(err)
	assert.True(taskId != uuid.UUID{})
	status, err := mgr.Status(taskId)
	assert.Nil(err)
	assert.Equal(status.Code, TransferStatusActive)

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

// This type implements core.Database with only enough behavior
// to test the task manager.
type FakeDatabase struct {
	StagingDuration time.Duration // time it takes to "stage files"
}

// creates a new fake database that stages its 3 files in 300 ms
func NewFakeDatabase() *FakeDatabase {
	db := FakeDatabase{
		StagingDuration: time.Duration(300) * time.Millisecond,
	}
	return &db
}

func (db *FakeDatabase) Search(params SearchParameters) (SearchResults, error) {
	// this method is unused, so we just need a placeholder
	return SearchResults{}, nil
}

func (db *FakeDatabase) Resources(fileIds []string) ([]DataResource, error) {
	resources := make([]DataResource, 3)
	var err error
	return resources, err
}

func (db *FakeDatabase) StageFiles(fileIds []string) (uuid.UUID, error) {
	return uuid.New(), nil
}

func (db *FakeDatabase) StagingStatus(id uuid.UUID) (StagingStatus, error) {
	return StagingStatusUnknown, nil
}

func (db *FakeDatabase) Endpoint() Endpoint {
	return NewFakeSourceEndpoint()
}

type TransferInfo struct {
	Time   time.Time // transfer initiation time
	Status TransferStatus
}

// This type implements core.Endpoint with only enough behavior
// to test the task manager.
type FakeEndpoint struct {
	TransferDuration time.Duration // time it takes to "transfer files"
	Source           bool          // true if this endpoint represents a source, false if not
	Xfers            map[uuid.UUID]TransferInfo
}

// creates a new fake source endpoint that transfers a fictional payload in 1
// second
func NewFakeSourceEndpoint() *FakeEndpoint {
	return &FakeEndpoint{
		TransferDuration: time.Second,
		Source:           true,
		Xfers:            make(map[uuid.UUID]TransferInfo),
	}
}

// creates a new fake destination endpoint
func NewFakeDestinationEndpoint() *FakeEndpoint {
	return &FakeEndpoint{
		TransferDuration: time.Second,
		Source:           false,
	}
}

func (ep *FakeEndpoint) FilesStaged(files []DataResource) (bool, error) {
	if ep.Source {
		// the source endpoint should report true for the staged files as long
		// as the source database has had time to stage them
	}
	return true, nil // FIXME
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
		return info.Status, nil
	} else {
		return TransferStatus{}, fmt.Errorf("Invalid transfer ID: %s", id.String())
	}
}

func (ep *FakeEndpoint) Cancel(id uuid.UUID) error {
	// not used (yet)
	return nil
}
