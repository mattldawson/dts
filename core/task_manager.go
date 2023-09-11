package core

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// this type holds multiple (possibly null) UUIDs corresponding to different
// portions of a file transfer
type taskType struct {
	Source, Destination Database       // source and destination databases
	FileIds             []string       // IDs of files within Source
	Resources           []DataResource // Frictionless DataResources for files
	Staging, Transfer   uuid.NullUUID  // staging and file transfer UUIDs (if any)
	Finished            bool           // true iff the task has completed
}

// this type holds various channels used by the TaskManager to communicate
// with its worker goroutine
type channelsType struct {
	TaskId chan uuid.UUID      // task ID channel
	Task   chan taskType       // task input channel
	Status chan TransferStatus // status output channel
	Error  chan error          // error output channel
	Poll   chan struct{}       // polling channel
	Stop   chan struct{}       // stop channel
}

func newChannels() channelsType {
	return channelsType{
		TaskId: make(chan uuid.UUID, 32),
		Task:   make(chan taskType, 32),
		Status: make(chan TransferStatus, 32),
		Error:  make(chan error, 32),
		Poll:   make(chan struct{}),
		Stop:   make(chan struct{}),
	}
}

// this function runs in its own goroutine, using channels to communicate with
// its TaskManager
func processTasks(channels channelsType) {
	// here's a table of transfer-related tasks
	tasks := make(map[uuid.UUID]taskType)

	// parse the channels into directional types as needed
	var taskIdChan chan uuid.UUID = channels.TaskId
	var taskChan <-chan taskType = channels.Task
	var statusChan chan<- TransferStatus = channels.Status
	var errorChan chan<- error = channels.Error
	var pollChan <-chan struct{} = channels.Poll
	var stopChan <-chan struct{} = channels.Stop

	// start scurrying around
	var err error
	for {
		select {
		case newTask := <-taskChan: // Add() called
			// resolve file paths using file IDs
			newTask.Resources, err = newTask.Source.Resources(newTask.FileIds)
			if err == nil {
				// tell the source DB to stage the files, stash the task, and return
				// its new ID
				newTask.Staging.UUID, err = newTask.Source.StageFiles(newTask.FileIds)
				if err == nil {
					xferId := uuid.New()
					tasks[xferId] = newTask
					taskIdChan <- xferId
				}
			}
			if err != nil {
				errorChan <- err
			}
		case taskId := <-taskIdChan: // Status() called
			if task, found := tasks[taskId]; found {
				var status TransferStatus
				if task.Staging.Valid { // files are being staged
					status = TransferStatus{
						Code:     TransferStatusStaging,
						NumFiles: len(task.FileIds),
					}
				} else if task.Transfer.Valid {
					status, err = task.Source.Endpoint().Status(task.Transfer.UUID)
					if status.Code == TransferStatusSucceeded || status.Code == TransferStatusFailed {
						task.Finished = true
						tasks[taskId] = task
					}
				} else {
					err = fmt.Errorf("Task %s status in invalid state!", taskId.String())
				}
				if err == nil {
					statusChan <- status
				} else {
					errorChan <- err
				}
			} else {
				errorChan <- fmt.Errorf("Task %s not found!", taskId.String())
			}
		case <-pollChan: // time to update tasks statuses
			var status TransferStatus
			for taskId, task := range tasks {
				var staged bool
				if !task.Finished {
					endpoint := task.Source.Endpoint()
					if task.Staging.Valid {
						// are the files staged?
						staged, err = endpoint.FilesStaged(task.Resources)
						if staged { // yup -- start the transfer
							fileXfers := make([]FileTransfer, len(task.Resources))
							for i, resource := range task.Resources {
								fileXfers[i] = FileTransfer{
									SourcePath:      resource.Path,
									DestinationPath: resource.Path,
									Hash:            resource.Hash,
								}
							}
							task.Transfer.UUID, err = endpoint.Transfer(task.Destination.Endpoint(), fileXfers)
							if err == nil {
								task.Staging.Valid = false
								task.Transfer.Valid = true
							}
						}
					} else if task.Transfer.Valid {
						// has the task completed?
						status, err = endpoint.Status(task.Transfer.UUID)
						if err == nil && (status.Code == TransferStatusSucceeded || status.Code == TransferStatusFailed) {
							task.Finished = true
							tasks[taskId] = task
						}
					}
					if err != nil {
						// FIXME: log the issue!
					}
				}
			}
		case <-stopChan: // Close() called
			break
		}
	}
}

// This type manages all the behind-the-scenes work involved in orchestrating
// the staging and transfer of files.
type TaskManager struct {
	PollInterval int // interval at which task manager checks statuses (seconds)
	Channels     channelsType
}

// creates a new task manager with the given poll interval
func NewTaskManager(pollInterval int) *TaskManager {
	mgr := TaskManager{
		PollInterval: pollInterval,
		Channels:     newChannels(),
	}

	go processTasks(mgr.Channels) // start processing tasks
	go func() {                   // start polling heartbeat
		time.Sleep(time.Duration(mgr.PollInterval) * time.Second)
		mgr.Channels.Poll <- struct{}{}
	}()
	return &mgr
}

// Adds a new transfer task to the manager's set, returning a UUID
// for the task. The task is defined by specifying source and destination
// databases and a set of file IDs associated with the source.
func (mgr *TaskManager) Add(source, destination Database, fileIDs []string) (uuid.UUID, error) {
	var taskId uuid.UUID
	var err error
	mgr.Channels.Task <- taskType{
		Source:      source,
		Destination: destination,
		FileIds:     fileIDs,
	}
	select {
	case taskId = <-mgr.Channels.TaskId:
	case err = <-mgr.Channels.Error:
	}
	return taskId, err
}

// given a task UUID, returns its transfer status code (or a non-nil error
// indicating any issues encountered)
func (mgr *TaskManager) Status(taskId uuid.UUID) (TransferStatus, error) {
	var status TransferStatus
	var err error
	mgr.Channels.TaskId <- taskId
	select {
	case status = <-mgr.Channels.Status:
	case err = <-mgr.Channels.Error:
	}
	return status, err
}

// Halts the task manager
func (mgr *TaskManager) Close() {
	mgr.Channels.Stop <- struct{}{}
}
