package services

import (
	"fmt"

	"github.com/google/uuid"

	"dts/core"
)

// this type holds multiple (possibly null) UUIDs corresponding to different
// portions of a file transfer
type task struct {
	// source database
	SourceDatabase core.Database
	// IDs and paths of files to be transferred (within the database)
	FileIds, FilePaths []string
	// staging and file transfer UUIDs (if any)
	Staging, Transfer uuid.NullUUID
}

// this type is used by mgr.Close() to indicate that the TaskManager should
// halt
type stop = struct{}

// this function runs in its own goroutine, using channels to communicate with
// its TaskManager
func processTasks(idChan chan uuid.UUID, taskIn chan<- task,
	statusOut <-chan core.TransferStatus, errOut <-chan error,
	stopChan chan<- stop) {
	// here's a table of transfer-related tasks
	tasks := map[uuid.UUID]task

	for {
		select {
		case newTask := <-taskIn: // Add() called
			// resolve file paths using file IDs
			newTask.FilePaths, err := newTask.Source.FilePaths(newTask.FileIds)
			if err != nil {
				errOut <- err
			}
			// tell the source DB to stage the files, stash the task, and return
			// its new ID
			newTask.Staging = newTask.Source.StageFiles(newTask.FileIds)
			xferId := uuid.New()
			tasks[xferId] = newTask
			idChan <- xferId
		case taskId := <-taskIdIn: // Status() called
			if task, found := tasks[taskId]; found {
				if task.Staging.Valid { // files are being staged
					statusOut <- core.TransferStatus{
						StatusCode:          core.TransferStatusStaging,
						NumFiles:            len(task.FileIds),
						NumFilesTransferred: 0,
					}
				} else if task.Transfer.Valid {
					// determine the status of this task
					endpoint := task.Source.Endpoint()
					status := task.Source.Endpoint().Status(task.Transfer.UUID)
					statusOut <- status
				} else {
					errChan <- fmt.Error("Task status in invalid state!")
				}
			} else {
				errChan <- fmt.Errorf("Task not found!")
			}
		case <-stopChan: // Close() called
			break
		}
	}
}

// this type is a proxy to the processTasks function, communicating with
// processTasks via channels
type TaskManager struct {
	// task ID channel (bidirectional)
	IdChan chan uuid.UUID
	// task input channel
	TaskIn chan<- task
	// status output channel
	StatusOut <-chan core.TransferStatus
	// error output channel
	ErrOut <-chan error
	// stop channel
	StopChan chan<- stop
}

// creates a new task manager
func NewTaskManager() *TaskManager {
	mgr := TaskManager{
		IdChan:    make(chan uuid.UUID),
		TaskIn:    make(chan<- task),
		StatusOut: make(<-chan core.TransferStatus),
		ErrOut:    make(<-chan error),
		StopChan:  make(chan<- stop),
	}
	go processTasks(mgr.IdChan, mgr.TaskIn, mgr.TaskIdIn, mgr.StatusOut, mgr.ErrOut, mgr.StopChan)
	return &mgr
}

// adds a new transfer task to the manager's set, consisting of
// * the name of the source database containing the files
// * a list of IDs for the files to be transferred from this database
func (mgr *TaskManager) Add(source core.Database, fileIDs []string) {
	mgr.TaskIn <- task{
		SourceDatabase: source,
		FilePaths:      fileIDs, // IDs are converted to paths asynchronously
	}
}

// given a task UUID, returns its transfer status code (or a non-nil error
// indicating any issues encountered)
func (mgr *TaskManager) Status(taskId uuid.UUID) (core.TransferStatus, error) {
	var status core.TransferStatus
	var err error
	mgr.TaskIdIn <- taskId
	select {
	case status = <-mgr.StatusOut:
	case err = <-mgr.ErrOut:
	}
	return status, err
}

// Halts the task manager
func (mgr *TaskManager) Close() {
	mgr.StopChan <- stop{}
}
