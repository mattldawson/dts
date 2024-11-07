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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
)

// This type tracks the lifecycle of a file transfer task that copies files from
// a source database to a destination database. A TransferTask can have one or
// more subtasks, depending on how many transfer endpoints are involved.
type TransferTask struct {
	Canceled          bool              // set if a cancellation request has been made
	CompletionTime    time.Time         // time at which the transfer completed
	Description       string            // Markdown description of the task
	Destination       string            // name of destination database (in config)
	DestinationFolder string            // folder path to which files are transferred
	FileIds           []string          // IDs of all files being transferred
	Id                uuid.UUID         // task identifier
	Instructions      json.RawMessage   // machine-readable task processing instructions
	Manifest          uuid.NullUUID     // manifest generation UUID (if any)
	ManifestFile      string            // name of locally-created manifest file
	PayloadSize       float64           // Size of payload (gigabytes)
	Source            string            // name of source database (in config)
	Status            TransferStatus    // status of file transfer operation
	Subtasks          []TransferSubtask // list of constituent file transfer subtasks
	UserInfo          auth.UserInfo     // info about user requesting transfer
}

// This error type is returned when a payload is requested that is too large.
type PayloadTooLargeError struct {
	size float64 // size of the requested payload in gigabytes
}

func (e PayloadTooLargeError) Error() string {
	return fmt.Sprintf("Requested payload is too large: %g GB (limit is %g GB).",
		e.size, config.Service.MaxPayloadSize)
}

// computes the size of a payload for a transfer task (in gigabytes)
func payloadSize(resources []DataResource) float64 {
	var size uint64
	for _, resource := range resources {
		size += uint64(resource.Bytes)
	}
	return float64(size) / float64(1024*1024*1024)
}

// starts a task going, initiating staging if needed
func (task *TransferTask) start() error {
	source, err := databases.NewDatabase(task.UserInfo.Orcid, task.Source)
	if err != nil {
		return err
	}

	// resolve resource data using file IDs
	resources, err := source.Resources(task.FileIds)
	if err != nil {
		return err
	}

	// make sure the size of the payload doesn't exceed our specified limit
	task.PayloadSize = payloadSize(resources) // (in GB)
	if task.PayloadSize > config.Service.MaxPayloadSize {
		return &PayloadTooLargeError{size: task.PayloadSize}
	}

	// determine the destination endpoint
	// FIXME: this conflicts with our redesign!!
	destinationEndpoint := config.Databases[task.Destination].Endpoint

	// construct a destination folder name
	destination, err := databases.NewDatabase(task.UserInfo.Orcid, task.Destination)
	if err != nil {
		return err
	}
	username, err := destination.LocalUser(task.UserInfo.Orcid)
	if err != nil {
		return err
	}
	task.DestinationFolder = filepath.Join(username, "dts-"+task.Id.String())

	// assemble distinct endpoints and create a subtask for each
	distinctEndpoints := make(map[string]interface{})
	for _, resource := range resources {
		if _, found := distinctEndpoints[resource.Endpoint]; !found {
			distinctEndpoints[resource.Endpoint] = struct{}{}
		}
	}
	task.Subtasks = make([]TransferSubtask, len(distinctEndpoints))
	for endpoint := range distinctEndpoints {
		// pick out the files corresponding to the source endpoint
		// NOTE: this is slow, but preserves file ID ordering
		resourcesForEndpoint := make([]DataResource, 0)
		for _, resource := range resources {
			if resource.Endpoint == endpoint {
				resourcesForEndpoint = append(resourcesForEndpoint, resource)
			}
		}

		// set up a subtask for the endpoint
		task.Subtasks = append(task.Subtasks, TransferSubtask{
			Destination:         task.Destination,
			DestinationEndpoint: destinationEndpoint,
			DestinationFolder:   task.DestinationFolder,
			Resources:           resourcesForEndpoint,
			Source:              task.Source,
			SourceEndpoint:      endpoint,
			UserInfo:            task.UserInfo,
		})
	}

	// start the subtasks
	for _, subtask := range task.Subtasks {
		subErr := subtask.start()
		if subErr != nil {
			err = subErr
		}
	}
	return err
}

// creates a DataPackage that serves as the transfer manifest
func (task *TransferTask) createManifest() DataPackage {
	resources := make([]DataResource, 0)
	for _, subtask := range task.Subtasks {
		n := len(resources)
		resources = resources[:len(subtask.Resources)]
		copy(resources[n:], subtask.Resources)
	}

	manifest := DataPackage{
		Name:      "manifest",
		Resources: resources,
		Created:   time.Now().Format(time.RFC3339),
		Profile:   "data-package",
		Keywords:  []string{"dts", "manifest"},
		Contributors: []Contributor{
			{
				Title:        task.UserInfo.Name,
				Email:        task.UserInfo.Email,
				Role:         "author",
				Organization: task.UserInfo.Organization,
			},
		},
		Description:  task.Description,
		Instructions: make(json.RawMessage, len(task.Instructions)),
	}
	copy(manifest.Resources, resources)
	copy(manifest.Instructions, task.Instructions)

	return manifest
}

// checks whether the file manifest for a task has been generated and, if so,
// marks the task as completed
func (task *TransferTask) checkManifest() error {
	// has the manifest transfer completed?
	localEndpoint, err := endpoints.NewEndpoint(config.Service.Endpoint)
	if err != nil {
		return err
	}
	xferStatus, err := localEndpoint.Status(task.Manifest.UUID)
	if err != nil {
		return err
	}
	if xferStatus.Code == TransferStatusSucceeded ||
		xferStatus.Code == TransferStatusFailed { // manifest transferred
		task.Manifest = uuid.NullUUID{}
		os.Remove(task.ManifestFile)
		task.ManifestFile = ""
		task.Status.Code = xferStatus.Code
		task.Status.Message = ""
		task.CompletionTime = time.Now()
	}
	return nil
}

// returns the duration since the task completed (successfully or otherwise),
// or 0 if the task has not completed
func (task TransferTask) Age() time.Duration {
	if task.Status.Code == TransferStatusSucceeded ||
		task.Status.Code == TransferStatusFailed {
		return time.Since(task.CompletionTime)
	} else {
		return time.Duration(0)
	}
}

// returns true if the task has completed (successfully or not), false otherwise
func (task TransferTask) completed() bool {
	for _, subtask := range task.Subtasks {
		if subtask.TransferStatus.Code != TransferStatusSucceeded &&
			subtask.TransferStatus.Code != TransferStatusFailed {
			return false
		}
	}
	return true
}

// requests that the task be canceled
func (task *TransferTask) cancel() error {
	task.Canceled = true                    // mark as canceled
	for _, subtask := range task.Subtasks { // cancel subtasks
		subtask.cancel()
	}
	return nil
}

// updates the state of a task, setting its status as necessary
func (task *TransferTask) update() error {
	var err error
	if len(task.Subtasks) == 0 { // new task!
		err = task.start()
	} else if task.Canceled { // cancellation requested
		for _, subtask := range task.Subtasks {
			err = subtask.checkCancellation()
		}
		if task.completed() {
			task.CompletionTime = time.Now()
		}
	} else if task.Manifest.Valid { // we're generating/sending a manifest
		err = task.checkManifest()
	} else { // update subtasks

		// track subtask failures
		var subtaskFailed bool
		var failedSubtaskStatus TransferStatus

		// update each subtask and check for failures
		subtaskStaging := false
		allTransfersSucceeded := true
		for _, subtask := range task.Subtasks {
			subErr := subtask.update()
			// FIXME: vvv is this the right thing to do?? vvv
			if subErr != nil {
				err = subErr
			}

			if subtask.StagingStatus == databases.StagingStatusFailed {
				subtaskFailed = true
				failedSubtaskStatus.Code = TransferStatusUnknown
				failedSubtaskStatus.Message = "task cancelled because of staging failure"
			} else if subtask.TransferStatus.Code == TransferStatusFailed {
				subtaskFailed = true
				failedSubtaskStatus.Code = TransferStatusFailed
				failedSubtaskStatus.Message = "task cancelled because of transfer failure"
			}
			if subtask.TransferStatus.Code != TransferStatusSucceeded {
				allTransfersSucceeded = false
			}
		}

		// if a subtask failed, cancel the task -- otherwise, update the task's
		// status based on those of its subtasks
		if subtaskFailed {
			// overwrite only the error code and message fields
			task.Status.Code = failedSubtaskStatus.Code
			task.Status.Message = failedSubtaskStatus.Message
			task.cancel()
		} else {
			// accumulate statistics
			task.Status.NumFiles = 0
			task.Status.NumFilesTransferred = 0
			task.Status.NumFilesSkipped = 0
			for _, subtask := range task.Subtasks {
				if subtask.Staging.Valid {
					subtaskStaging = true
				} else if subtask.Transfer.Valid {
					task.Status.NumFiles += subtask.TransferStatus.NumFiles
					task.Status.NumFilesTransferred += subtask.TransferStatus.NumFilesTransferred
					task.Status.NumFilesSkipped += subtask.TransferStatus.NumFilesSkipped
				}
			}
		}

		if subtaskStaging && task.Status.NumFiles == 0 {
			task.Status = TransferStatus{
				Code: TransferStatusStaging,
			}
		} else if allTransfersSucceeded { // write a manifest
			localEndpoint, err := endpoints.NewEndpoint(config.Service.Endpoint)
			if err != nil {
				return err
			}

			// generate a manifest for the transfer
			manifest := task.createManifest()

			// write the manifest to disk and begin transferring it to the
			// destination endpoint
			var manifestBytes []byte
			manifestBytes, err = json.Marshal(manifest)
			if err != nil {
				return fmt.Errorf("marshalling manifest content: %s", err.Error())
			}
			task.ManifestFile = filepath.Join(config.Service.ManifestDirectory,
				fmt.Sprintf("manifest-%s.json", task.Id.String()))
			manifestFile, err := os.Create(task.ManifestFile)
			if err != nil {
				return fmt.Errorf("creating manifest file: %s", err.Error())
			}
			_, err = manifestFile.Write(manifestBytes)
			if err != nil {
				return fmt.Errorf("writing manifest file content: %s", err.Error())
			}
			err = manifestFile.Close()
			if err != nil {
				return fmt.Errorf("closing manifest file: %s", err.Error())
			}

			// construct the source/destination file manifest paths
			fileXfers := []FileTransfer{
				{
					SourcePath:      task.ManifestFile,
					DestinationPath: filepath.Join(task.DestinationFolder, "manifest.json"),
				},
			}

			// begin transferring the manifest
			// FIXME: how do we determine the database's destination endpoint?
			destinationEndpointName, found := config.Databases[task.Destination].Endpoints["destination"]
			if !found {
				destinationEndpointName = config.Databases[task.Destination].Endpoint
			}
			destinationEndpoint, err := endpoints.NewEndpoint(destinationEndpointName)
			if err != nil {
				return err
			}
			task.Manifest.UUID, err = localEndpoint.Transfer(destinationEndpoint, fileXfers)
			if err != nil {
				return fmt.Errorf("transferring manifest file: %s", err.Error())
			}

			task.Status = TransferStatus{
				Code: TransferStatusFinalizing,
			}
			task.Manifest.Valid = true
		} else {
			// the transfer failed, so make sure we cancel it in case it's still
			// trying (because e.g. Globus continues trying transfers for ~3 days!!)
			task.cancel()
		}
	}
	return err
}
