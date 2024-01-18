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

package tasks

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/core"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
)

// useful type aliases
type Database = core.Database
type DataPackage = core.DataPackage
type DataResource = core.DataResource
type Endpoint = core.Endpoint
type FileTransfer = core.FileTransfer
type TransferStatus = core.TransferStatus

// useful constants
const (
	TransferStatusStaging    = core.TransferStatusStaging
	TransferStatusActive     = core.TransferStatusActive
	TransferStatusFailed     = core.TransferStatusFailed
	TransferStatusFinalizing = core.TransferStatusFinalizing
	TransferStatusInactive   = core.TransferStatusInactive
	TransferStatusSucceeded  = core.TransferStatusSucceeded
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
	CompletionTime      time.Time      // time at which the transfer completed
}

// starts a task going, initiating staging
func (task *taskType) start() error {
	// resolve file paths using file IDs
	var err error
	task.Resources, err = task.Source.Resources(task.FileIds)
	if err != nil {
		return err
	}

	// tell the source DB to stage the files, stash the task, and return
	// its new ID
	task.Staging.UUID, err = task.Source.StageFiles(task.FileIds)
	task.Staging.Valid = true
	if err != nil {
		return err
	}
	task.Status = TransferStatus{
		Code:     TransferStatusStaging,
		NumFiles: len(task.FileIds),
	}
	return nil
}

// checks whether files for a task are finished staging and, if so,
// initiates the transfer process
func (task *taskType) checkStaging() error {
	sourceEndpoint, err := task.Source.Endpoint()
	if err != nil {
		return err
	}
	staged, err := sourceEndpoint.FilesStaged(task.Resources)
	if err != nil {
		return err
	}
	if staged {
		// construct the source/destination file paths
		username, err := task.Source.LocalUser(task.Orcid)
		if err != nil {
			return err
		}
		fileXfers := make([]FileTransfer, len(task.Resources))
		for i, resource := range task.Resources {
			destinationPath := filepath.Join(username, task.Id.String(), resource.Path)
			fileXfers[i] = FileTransfer{
				SourcePath:      resource.Path,
				DestinationPath: destinationPath,
				Hash:            resource.Hash,
			}
		}

		// initiate the transfer
		destinationEndpoint, err := task.Destination.Endpoint()
		if err != nil {
			return err
		}
		task.Transfer.UUID, err = sourceEndpoint.Transfer(destinationEndpoint, fileXfers)
		if err != nil {
			return err
		}

		task.Status = TransferStatus{
			Code:     TransferStatusActive,
			NumFiles: len(task.FileIds),
		}
		task.Staging = uuid.NullUUID{}
		task.Transfer.Valid = true
	}
	return nil
}

// checks whether files for a task are finished transferring and, if so,
// initiates the generation of the file manifest
func (task *taskType) checkTransfer() error {
	// has the data transfer completed?
	sourceEndpoint, err := task.Source.Endpoint()
	if err != nil {
		return err
	}
	xferStatus, err := sourceEndpoint.Status(task.Transfer.UUID)
	if err != nil {
		return err
	}
	if xferStatus.Code == TransferStatusSucceeded ||
		xferStatus.Code == TransferStatusFailed { // transfer finished
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
			if err != nil {
				return err
			}
			var manifestFile *os.File
			manifestFile, err = os.CreateTemp(task.LocalEndpoint.Root(), "manifest.json")
			if err != nil {
				return err
			}
			_, err = manifestFile.Write(manifestBytes)
			if err != nil {
				return err
			}
			task.ManifestFile = manifestFile.Name()
			err = manifestFile.Close()
			if err != nil {
				return err
			}

			// construct the source/destination file manifest paths
			username, err := task.Source.LocalUser(task.Orcid)
			if err != nil {
				return err
			}
			fileXfers := []FileTransfer{
				FileTransfer{
					SourcePath:      filepath.Base(task.ManifestFile), // relative to root!
					DestinationPath: filepath.Join(username, task.Id.String(), "manifest.json"),
				},
			}

			// begin transferring the manifest
			destinationEndpoint, err := task.Destination.Endpoint()
			if err != nil {
				return err
			}
			task.Manifest.UUID, err = task.LocalEndpoint.Transfer(destinationEndpoint, fileXfers)
			if err != nil {
				return err
			}

			task.Status = TransferStatus{
				Code: TransferStatusFinalizing,
			}
			task.Manifest.Valid = true
		}
	}
	return nil
}

// checks whether the file manifest for a task has been generated and, if so,
// marks the task as completed
func (task *taskType) checkManifest() error {
	// has the manifest transfer completed?
	xferStatus, err := task.LocalEndpoint.Status(task.Manifest.UUID)
	if err != nil {
		return err
	}
	if xferStatus.Code == TransferStatusSucceeded ||
		xferStatus.Code == TransferStatusFailed { // manifest transferred
		task.Manifest = uuid.NullUUID{}
		os.Remove(task.ManifestFile)
		task.ManifestFile = ""
		task.Status.Code = xferStatus.Code
		task.CompletionTime = time.Now()
	}
	return nil
}

// returns the duration since the task completed (successfully or otherwise),
// or 0 if the task has not completed
func (task *taskType) Age() time.Duration {
	if task.Status.Code == TransferStatusSucceeded ||
		task.Status.Code == TransferStatusFailed {
		return time.Since(task.CompletionTime)
	} else {
		return time.Duration(0)
	}
}

// this function updates the state of a task, setting its status as necessary
func (task *taskType) Update() error {
	var err error
	if task.Resources == nil { // new task!
		err = task.start()
	} else if task.Staging.Valid { // we're staging
		err = task.checkStaging()
	} else if task.Transfer.Valid { // we're transferring
		err = task.checkTransfer()
	} else if task.Manifest.Valid { // we're generating/sending a manifest
		err = task.checkManifest()
	}
	return err
}

// loads a map of task IDs to tasks from a previously saved file if available,
// or creates an empty map if no such file is available or valid
func createOrLoadTasks(dataFile string) map[uuid.UUID]taskType {
	file, err := os.Open(dataFile)
	if err != nil {
		return make(map[uuid.UUID]taskType)
	}
	slog.Debug(fmt.Sprintf("Found previous tasks in %s.", dataFile))
	defer file.Close()
	enc := gob.NewDecoder(file)
	var tasks map[uuid.UUID]taskType
	err = enc.Decode(&tasks)
	if err != nil { // file not readable
		slog.Error(fmt.Sprintf("Reading task manager file %s: %s", dataFile, err.Error()))
		return make(map[uuid.UUID]taskType)
	}
	slog.Debug(fmt.Sprintf("Restored %d tasks from %s", len(tasks), dataFile))
	return tasks
}

// saves a map of task IDs to tasks to the given file
func saveTasks(tasks map[uuid.UUID]taskType, dataFile string) error {
	if len(tasks) > 0 {
		slog.Debug(fmt.Sprintf("Saving %d tasks to %s", len(tasks), dataFile))
		file, err := os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("Opening task manager file %s: %s", dataFile, err.Error())
		}
		enc := gob.NewEncoder(file)
		err = enc.Encode(tasks)
		if err != nil {
			file.Close()
			os.Remove(dataFile)
			return fmt.Errorf("Saving tasks: %s", err.Error())
		}
		err = file.Close()
		if err != nil {
			os.Remove(dataFile)
			return fmt.Errorf("Writing task manager file %s: %s", dataFile, err.Error())
		}
		slog.Debug(fmt.Sprintf("Saved %d tasks to %s", len(tasks), dataFile))
	}
	return nil
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

// this function runs in its own goroutine, using the given local endpoint
// for local file transfers, and the given channels to communicate with
// the TaskManager
func processTasks() {
	// create or recreate a persistent table of transfer-related tasks
	dataStore := filepath.Join(config.Service.DataDirectory, "dts.gob")
	tasks := createOrLoadTasks(dataStore)

	// parse the task channels into directional types as needed
	var taskIdChan chan uuid.UUID = taskChannels.TaskId
	var taskChan <-chan taskType = taskChannels.Task
	var statusChan chan<- TransferStatus = taskChannels.Status
	var errorChan chan<- error = taskChannels.Error
	var pollChan <-chan struct{} = taskChannels.Poll
	var stopChan <-chan struct{} = taskChannels.Stop

	// the task deletion period is specified in hours
	deleteAfter := time.Duration(config.Service.DeleteAfter) * time.Hour

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
				if err != nil {
					// we log task update errors but do not propagate them
					slog.Error(err.Error())
				}
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

				// if the task completed a long enough time go, delete its entry
				if task.Age() > deleteAfter {
					slog.Debug(fmt.Sprintf("Purging task %s.", task.Id.String()))
					delete(tasks, taskId)
				} else { // update its entry
					tasks[taskId] = task
				}
			}
		case <-stopChan: // Stop() called
			err := saveTasks(tasks, dataStore) // don't forget to save our state!
			errorChan <- err
			break
		}
	}
}

// this function sends a regular pulse on its poll channel until it receives a
// pulse on its stop channel
func heartbeat(pollInterval time.Duration, pollChan chan<- struct{},
	stopChan <-chan struct{}) {
	for {
		time.Sleep(pollInterval)
		pollChan <- struct{}{}
		select {
		case <-stopChan: // Stop() called
			break
		}
	}
}

// this function checks for the existence of the data directory and whether it
// is readable/writeable, returning a non-nil error if any of these conditions
// are not met
func validateDataDirectory(dir string) error {
	if dir == "" {
		return fmt.Errorf("no data directory was specified!")
	}
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("Given data directory is not a directory!")
	}

	// can we write a file and read it?
	testFile := filepath.Join(dir, "test.txt")
	writtenTestData := []byte("test")
	err = os.WriteFile(testFile, writtenTestData, 0644)
	if err != nil {
		return fmt.Errorf("Could not write to given data directory!")
	}
	readTestData, err := os.ReadFile(testFile)
	if err == nil {
		os.Remove(testFile)
	}
	if err != nil || !bytes.Equal(readTestData, writtenTestData) {
		return fmt.Errorf("Could not read from given data directory!")
	}
	return nil
}

// global variables for managing tasks
var running bool                // true if tasks are processing, false if not
var taskChannels channelsType   // channels used for processing tasks
var stopHeartbeat chan struct{} // send a pulse to this channel to halt polling

// Starts processing tasks according to the given configuration, returning an
// informative error if anything prevents this.
func Start() error {
	//localEndpoint Endpoint, pollInterval time.Duration,
	//dataDirectory string, deleteAfter time.Duration) (*TaskManager, error) {

	if running {
		return fmt.Errorf("Tasks are already running and cannot be started again.")
	}

	// does the directory exist and is it writable/readable?
	err := validateDataDirectory(config.Service.DataDirectory)
	if err != nil {
		return err
	}

	// allocate channels
	taskChannels = channelsType{
		TaskId: make(chan uuid.UUID, 32),
		Task:   make(chan taskType, 32),
		Status: make(chan TransferStatus, 32),
		Error:  make(chan error, 32),
		Poll:   make(chan struct{}),
		Stop:   make(chan struct{}),
	}

	// start processing tasks
	go processTasks()

	// start the polling heartbeat
	pollInterval := time.Duration(config.Service.PollInterval) * time.Millisecond
	stopHeartbeat = make(chan struct{})
	go heartbeat(pollInterval, taskChannels.Poll, stopHeartbeat)

	return nil
}

// Stops processing tasks. Adding new tasks and requesting task statuses are
// disallowed in a stopped state.
func Stop() {
	taskChannels.Stop <- struct{}{}
	stopHeartbeat <- struct{}{}
	err := <-taskChannels.Error
	if err != nil {
		slog.Error(err.Error())
	}
}

// Returns true if tasks are currently being processed, false if not.
func Running() bool {
	return running
}

// Adds a new transfer task associated with the user with the specified Orcid ID
// to the manager's set, returning a UUID for the task. The task is defined by
// specifying the names of the source and destination databases and a set of
// file IDs associated with the source.
func Add(orcid, source, destination string, fileIDs []string) (uuid.UUID, error) {
	var taskId uuid.UUID

	// fetch source and destination databases and our local endpoint
	sourceDb, err := databases.NewDatabase(orcid, source)
	if err != nil {
		return taskId, err
	}
	destinationDb, err := databases.NewDatabase(orcid, destination)
	if err != nil {
		return taskId, err
	}
	localEndpoint, err := endpoints.NewEndpoint(config.Service.Endpoint)
	if err != nil {
		return taskId, err
	}

	// create a new task and send it along for processing
	taskChannels.Task <- taskType{
		Orcid:         orcid,
		Source:        sourceDb,
		Destination:   destinationDb,
		LocalEndpoint: localEndpoint,
		FileIds:       fileIDs,
	}
	select {
	case taskId = <-taskChannels.TaskId:
	case err = <-taskChannels.Error:
	}
	return taskId, err
}

// given a task UUID, returns its transfer status code (or a non-nil error
// indicating any issues encountered)
func Status(taskId uuid.UUID) (TransferStatus, error) {
	var status TransferStatus
	var err error
	taskChannels.TaskId <- taskId
	select {
	case status = <-taskChannels.Status:
	case err = <-taskChannels.Error:
	}
	return status, err
}
