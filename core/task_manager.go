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

package core

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

// this type holds multiple (possibly null) UUIDs corresponding to different
// portions of a file transfer
type taskType struct {
	Id                  uuid.UUID      // task identifier
	Orcid               string         // Orcid ID for user requesting transfer
	Source, Destination Database       // source and destination databases
	LocalEndpoint       Endpoint       // local endpoint used for manifest transfers
	FileIds             []string       // IDs of files within Source
	Resources           []DataResource // Frictionless DataResources for files
	Staging, Transfer   uuid.NullUUID  // staging and file transfer UUIDs (if any)
	Manifest            uuid.NullUUID  // manifest generation UUID (if any)
	ManifestFile        string         // name of locally-created manifest file
	Status              TransferStatus // status of file transfer operation
}

// This function updates the state of a task, setting its status as necessary.
// By modern standards, this function may seem long, but I don't think breaking
// it into disparate pieces makes it any easier to understand, given the state
// information being managed--that just creates other abstraction problems.
func (task *taskType) Update() error {
	username := "user" // FIXME: how do we obtain this from our Orcid ID?
	sourceEndpoint, err := task.Source.Endpoint()
	if err != nil {
		return err
	}
	destinationEndpoint, err := task.Destination.Endpoint()
	if err != nil {
		return err
	}

	if task.Resources == nil { // new task!
		// resolve file paths using file IDs
		task.Resources, err = task.Source.Resources(task.FileIds)
		if err == nil {
			// tell the source DB to stage the files, stash the task, and return
			// its new ID
			task.Staging.UUID, err = task.Source.StageFiles(task.FileIds)
			task.Staging.Valid = true
			if err == nil {
				task.Status = TransferStatus{
					Code:     TransferStatusStaging,
					NumFiles: len(task.FileIds),
				}
			}
		}
	} else if task.Staging.Valid { // we're staging
		// are the files staged?
		var staged bool
		staged, err = sourceEndpoint.FilesStaged(task.Resources)
		if err == nil && staged {
			// initiate the transfer
			fileXfers := make([]FileTransfer, len(task.Resources))
			for i, resource := range task.Resources {
				destinationPath := filepath.Join(username, task.Id.String(), resource.Path)
				fileXfers[i] = FileTransfer{
					SourcePath:      resource.Path,
					DestinationPath: destinationPath,
					Hash:            resource.Hash,
				}
			}
			task.Transfer.UUID, err = sourceEndpoint.Transfer(destinationEndpoint, fileXfers)
			if err == nil {
				task.Status = TransferStatus{
					Code:     TransferStatusActive,
					NumFiles: len(task.FileIds),
				}
				task.Staging = uuid.NullUUID{}
				task.Transfer.Valid = true
			}
		}
	} else if task.Transfer.Valid { // we're transferring
		// has the data transfer completed?
		var xferStatus TransferStatus
		xferStatus, err = sourceEndpoint.Status(task.Transfer.UUID)
		if err == nil && (xferStatus.Code == TransferStatusSucceeded ||
			xferStatus.Code == TransferStatusFailed) { // transfer finished
			task.Transfer = uuid.NullUUID{}
			if xferStatus.Code == TransferStatusSucceeded {
				// generate a manifest for the transfer
				manifest := DataPackage{
					Name:      "manifest",
					Resources: make([]DataResource, len(task.Resources)),
				}
				copy(manifest.Resources, task.Resources)

				// write the manifest to disk and begin transferring it to the
				// destination endpoint
				var manifestBytes []byte
				manifestBytes, err = json.Marshal(manifest)
				if err == nil {
					var manifestFile *os.File
					manifestFile, err = os.CreateTemp(task.LocalEndpoint.Root(), "manifest.json")
					if err == nil {
						_, err = manifestFile.Write(manifestBytes)
						if err == nil {
							task.ManifestFile = manifestFile.Name()
							err = manifestFile.Close()
							if err == nil {
								// begin the transfer
								fileXfers := []FileTransfer{
									FileTransfer{
										SourcePath:      filepath.Base(task.ManifestFile), // relative to root!
										DestinationPath: filepath.Join(username, task.Id.String(), "manifest.json"),
									},
								}
								task.Manifest.UUID, err = task.LocalEndpoint.Transfer(destinationEndpoint, fileXfers)
								if err == nil {
									task.Status = TransferStatus{
										Code: TransferStatusFinalizing,
									}
									task.Manifest.Valid = true
								}
							}
						}
					}
				}
			}
		}
	} else if task.Manifest.Valid { // we're generating/sending a manifest
		// has the manifest transfer completed?
		var xferStatus TransferStatus
		xferStatus, err = task.LocalEndpoint.Status(task.Manifest.UUID)
		if err == nil && (xferStatus.Code == TransferStatusSucceeded ||
			xferStatus.Code == TransferStatusFailed) { // transfer finished
			task.Manifest = uuid.NullUUID{}
			os.Remove(task.ManifestFile)
			task.ManifestFile = ""
			task.Status.Code = xferStatus.Code
		}
	}
	return err
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

// this function runs in its own goroutine, using the given local endpoint
// for local file transfers, and the given channels to communicate with
// the TaskManager
func processTasks(channels channelsType) {
	// here's a persistent table of transfer-related tasks
	tasks := make(map[uuid.UUID]taskType)

	// parse the channels into directional types as needed
	var taskIdChan chan uuid.UUID = channels.TaskId
	var taskChan <-chan taskType = channels.Task
	var statusChan chan<- TransferStatus = channels.Status
	var errorChan chan<- error = channels.Error
	var pollChan <-chan struct{} = channels.Poll
	var stopChan <-chan struct{} = channels.Stop

	// start scurrying around
	for {
		select {
		case newTask := <-taskChan: // Add() called
			newTask.Id = uuid.New()
			tasks[newTask.Id] = newTask
			taskIdChan <- newTask.Id
			slog.Info(fmt.Sprintf("Added new transfer task %s.", newTask.Id.String()))
		case taskId := <-taskIdChan: // Status() called
			if task, found := tasks[taskId]; found {
				statusChan <- task.Status
			} else {
				err := fmt.Errorf("Task %s not found!", taskId.String())
				errorChan <- err
			}
		case <-pollChan: // time to move things along
			for taskId, task := range tasks {
				oldStatus := task.Status
				err := task.Update()
				if err == nil {
					if task.Status.Code != oldStatus.Code {
						switch task.Status.Code {
						case TransferStatusStaging:
							slog.Info(fmt.Sprintf("Staging files for task %s.", task.Id.String()))
						case TransferStatusActive:
							slog.Info(fmt.Sprintf("Beginning transfer for task %s.", task.Id.String()))
						case TransferStatusInactive:
							slog.Info(fmt.Sprintf("Suspended transfer for task %s.", task.Id.String()))
						case TransferStatusFinalizing:
							slog.Info(fmt.Sprintf("Finalizing transfer for task %s.", task.Id.String()))
						case TransferStatusSucceeded:
							slog.Info(fmt.Sprintf("Transfer task %s completed successfully.", task.Id.String()))
						case TransferStatusFailed:
							slog.Info(fmt.Sprintf("Transfer task %s failed.", task.Id.String()))
						}
					}
					tasks[taskId] = task
				} else {
					// we log task update errors but do not propagate them
					slog.Error(err.Error())
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
	LocalEndpoint Endpoint      // local endpoint used for transferring manifests
	PollInterval  time.Duration // interval at which task manager checks statuses
	Channels      channelsType
}

// creates a new task manager with the given local endpoint and poll interval
func NewTaskManager(localEndpoint Endpoint, pollInterval time.Duration) (*TaskManager, error) {
	if pollInterval <= 0 {
		return nil, fmt.Errorf("non-positive poll interval specified!")
	}

	mgr := TaskManager{
		LocalEndpoint: localEndpoint,
		PollInterval:  pollInterval,
		Channels:      newChannels(),
	}

	go processTasks(mgr.Channels) // start processing tasks
	go func() {                   // start polling heartbeat
		for {
			time.Sleep(mgr.PollInterval)
			mgr.Channels.Poll <- struct{}{}
		}
	}()
	return &mgr, nil
}

// Adds a new transfer task associated with the user with the specified Orcid ID
// to the manager's set, returning a UUID for the task. The task is defined by
// specifying source and destination databases and a set of file IDs associated
// with the source.
func (mgr *TaskManager) Add(orcid string, source, destination Database, fileIDs []string) (uuid.UUID, error) {
	var taskId uuid.UUID
	var err error
	mgr.Channels.Task <- taskType{
		LocalEndpoint: mgr.LocalEndpoint,
		Orcid:         orcid,
		Source:        source,
		Destination:   destination,
		FileIds:       fileIDs,
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
