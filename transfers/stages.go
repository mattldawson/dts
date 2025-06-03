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

package transfers

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/deliveryhero/pipeline/v2"
	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
)

//-----------------
// Pipeline Stages
//-----------------

// Call these functions within your pipeline's goroutine to establish stages. Stages are simply
// goroutines that asynchronously process work, chained together in sequence by their input/output
// channels.
//
// These pipeline stages are a bit unusual in that the work is not very processor-intensive (since
// it occurs elsewhere), but typically takes a very long time. Each transfer is processed
// concurrently and each long-running stage has polling interval at which it checks the progress of
// its work. There can be a very large number of concurrent pipelines (corresponding to the number
// of outstanding transfers), because each transfer occupies its own goroutine. It's unlikely that
// the number of concurrent DTS transfers will exceed the maximum number of Goroutines on any
// reasonable hardware, since they're very cheap.

// passed to the initial stage to create new transfers
type IdAndSpecification struct {
	Id            uuid.UUID
	Specification Specification
}

// channels used by pipeline stages for communication with the dispatch
type StageChannels struct {
	Cancel   chan uuid.UUID              // receives cancellation requests
	Complete chan Task                   // for reporting task completion
	Update   chan<- TransferStatusUpdate // for providing status updates
	Error    chan<- error                // for reporting errors
}

// creates a new Transfer with the given ID from the given Specification
func CreateNewTransfer(channels StageChannels) pipeline.Processor[IdAndSpecification, Transfer] {
	process := func(ctx context.Context, idAndSpec IdAndSpecification) (Transfer, error) {
		// access the source database, resolving resource descriptors
		source, err := databases.NewDatabase(idAndSpec.Specification.Source)
		if err != nil {
			return Transfer{}, err
		}

		transfer := Transfer{
			Id:            idAndSpec.Id,
			Specification: idAndSpec.Specification,
			Status: TransferStatus{
				Code:     TransferStatusNew,
				NumFiles: len(idAndSpec.Specification.FileIds),
			},
			Tasks: make([]Task, 0),
		}

		transfer.DataDescriptors = make([]any, 0)
		{
			descriptors, err := source.Descriptors(transfer.Specification.User.Orcid, transfer.Specification.FileIds)
			if err != nil {
				return Transfer{}, err
			}

			// sift through the descriptors and separate files from in-line data
			for _, descriptor := range descriptors {
				if _, found := descriptor["path"]; found { // file to be transferred
					transfer.DataDescriptors = append(transfer.DataDescriptors, descriptor)
				} else if _, found := descriptor["data"]; found { // inline data
					transfer.DataDescriptors = append(transfer.DataDescriptors, descriptor)
				} else { // neither!
					err = fmt.Errorf("Descriptor '%s' (ID: %s) has no 'path' or 'data' field!",
						descriptor["name"], descriptor["id"])
					break
				}
			}
			if err != nil {
				return Transfer{}, err
			}
		}

		// if the database stores its files in more than one location, check that each
		// resource is associated with a valid endpoint
		if len(config.Databases[transfer.Specification.Source].Endpoints) > 1 {
			for _, d := range transfer.DataDescriptors {
				descriptor := d.(map[string]any)
				id := descriptor["id"].(string)
				endpoint := descriptor["endpoint"].(string)
				if endpoint == "" {
					return Transfer{}, &databases.ResourceEndpointNotFoundError{
						Database:   transfer.Specification.Source,
						ResourceId: id,
					}
				}
				if _, found := config.Endpoints[endpoint]; !found {
					return Transfer{}, &databases.InvalidResourceEndpointError{
						Database:   transfer.Specification.Source,
						ResourceId: id,
						Endpoint:   endpoint,
					}
				}
			}
		} else { // otherwise, just assign the database's endpoint to the resources
			for _, d := range transfer.DataDescriptors {
				descriptor := d.(map[string]any)
				descriptor["endpoint"] = config.Databases[transfer.Specification.Source].Endpoint
			}
		}

		// make sure the size of the payload doesn't exceed our specified limit
		transfer.PayloadSize = payloadSize(transfer.DataDescriptors) // (in GB)
		if transfer.PayloadSize > config.Service.MaxPayloadSize {
			return Transfer{}, &PayloadTooLargeError{Size: transfer.PayloadSize}
		}

		// determine the destination endpoint
		// FIXME: this conflicts with our redesign!!
		destinationEndpoint := config.Databases[transfer.Specification.Destination].Endpoint

		// construct a destination folder name
		destination, err := databases.NewDatabase(transfer.Specification.Destination)
		if err != nil {
			return Transfer{}, err
		}

		username, err := destination.LocalUser(transfer.Specification.User.Orcid)
		if err != nil {
			return Transfer{}, err
		}
		transfer.DestinationFolder = filepath.Join(username, "dts-"+transfer.Id.String())

		// assemble distinct endpoints and create a task for each
		distinctEndpoints := make(map[string]any)
		for _, d := range transfer.DataDescriptors {
			descriptor := d.(map[string]any)
			endpoint := descriptor["endpoint"].(string)
			if _, found := distinctEndpoints[endpoint]; !found {
				distinctEndpoints[endpoint] = struct{}{}
			}
		}
		for sourceEndpoint := range distinctEndpoints {
			// pick out the files corresponding to the source endpoint
			// NOTE: this is slow, but preserves file ID ordering
			descriptorsForEndpoint := make([]any, 0)
			for _, d := range transfer.DataDescriptors {
				descriptor := d.(map[string]any)
				endpoint := descriptor["endpoint"].(string)
				if endpoint == sourceEndpoint {
					descriptorsForEndpoint = append(descriptorsForEndpoint, descriptor)
				}
			}

			// set up a task for the endpoint
			transfer.Tasks = append(transfer.Tasks, Task{
				TransferId:          transfer.Id,
				Index:               len(transfer.Tasks),
				Destination:         transfer.Specification.Destination,
				DestinationEndpoint: destinationEndpoint,
				DestinationFolder:   transfer.DestinationFolder,
				Descriptors:         descriptorsForEndpoint,
				Source:              transfer.Specification.Source,
				SourceEndpoint:      sourceEndpoint,
				Status: TaskStatus{
					Code: TaskStatusNew,
				},
				User: transfer.Specification.User,
			})
		}

		return transfer, nil
	}
	cancel := func(idAndSpec IdAndSpecification, err error) {
		// send all errors to our error reporting channel
		channels.Error <- err
	}
	return pipeline.NewProcessor(process, cancel)
}

// dispatches a transfer to the appropriate provider based on its source and destination
func DispatchToProvider(providers map[string]ProviderSequence, channels StageChannels) pipeline.Processor[Transfer, Transfer] {
	process := func(ctx context.Context, transfer Transfer) (Transfer, error) {
		key := fmt.Sprintf("%s->%s", transfer.Specification.Source, transfer.Specification.Destination)
		if provider, found := providers[key]; found {
			// set up a pipeline for exclusive use by this transfer's tasks
			input := provider.Channels.Dispatch
			xferCtx, stop := context.WithCancel(ctx)
			numTasks := len(transfer.Tasks)
			output := pipeline.ProcessConcurrently(xferCtx, numTasks, provider.Sequence, input)

			// dispatch the transfers
			for _, task := range transfer.Tasks {
				input <- task
			}

			// wait for all the cows to come home
			numCompleted := 0
			for numCompleted < numTasks {
				select {
				case completedTask := <-output:
					transfer.Tasks[completedTask.Index] = completedTask
					numCompleted++
				case transferId := <-channels.Cancel:
					if transferId == transfer.Id {
						slog.Info(fmt.Sprintf("Transfer %s cancelled.", transferId.String()))
						stop() // FIXME: this doesn't actually cancel e.g. Globus transfers
					}
				}
			}
			stop() // shut down our transfer-specific context on completion
		} else {
			return Transfer{}, InvalidSourceOrDestinationError(transfer.Specification.Source, transfer.Specification.Destination)
		}

		return transfer, nil
	}
	cancel := func(transfer Transfer, err error) {
		// send all errors to our error reporting channel
		channels.Error <- err
	}
	return pipeline.NewProcessor(process, cancel)
}

// moves files into place at the source, for transfer elsewhere
func StageFilesAtSource(channels StageChannels) pipeline.Processor[Task, Task] {
	process := func(ctx context.Context, task Task) (Task, error) {
		// check whether files are in place
		sourceEndpoint, err := endpoints.NewEndpoint(task.SourceEndpoint)
		if err != nil {
			return Task{}, err
		}
		staged, err := sourceEndpoint.FilesStaged(task.Descriptors)
		if err != nil {
			return Task{}, err
		}

		if !staged {
			// tell the source DB to stage the files, stash the task, and return
			// its new ID
			source, err := databases.NewDatabase(task.Source)
			if err != nil {
				return Task{}, err
			}
			fileIds := make([]string, len(task.Descriptors))
			for i, d := range task.Descriptors {
				descriptor := d.(map[string]any)
				fileIds[i] = descriptor["id"].(string)
			}
			taskId, err := source.StageFiles(task.User.Orcid, fileIds)
			if err != nil {
				return Task{}, err
			}
			task.Status.StagingId = uuid.NullUUID{
				UUID:  taskId,
				Valid: true,
			}
			task.Status.TransferStatus = endpoints.TransferStatus{
				Code:     endpoints.TransferStatusStaging,
				NumFiles: len(task.Descriptors),
			}
		}

		// wait till the files are staged
		for !staged {
			time.Sleep(time.Duration(config.Service.PollInterval) * time.Millisecond)
			staged, err = sourceEndpoint.FilesStaged(task.Descriptors)
			if err != nil {
				return Task{}, err
			}
		}

		return task, nil
	}
	cancel := func(task Task, err error) {
		// send all errors to our error reporting channel
		channels.Error <- err
	}
	return pipeline.NewProcessor(process, cancel)
}

func TransferToDestination(channels StageChannels) pipeline.Processor[Task, Task] {
	process := func(ctx context.Context, task Task) (Task, error) {
		slog.Debug(fmt.Sprintf("Transferring %d file(s) from %s to %s",
			len(task.Descriptors), task.SourceEndpoint, task.DestinationEndpoint))

		// assemble a list of file transfers
		fileXfers := make([]FileTransfer, len(task.Descriptors))
		for i, d := range task.Descriptors {
			descriptor := d.(map[string]any)
			path := descriptor["path"].(string)
			destinationPath := filepath.Join(task.DestinationFolder, path)
			fileXfers[i] = FileTransfer{
				SourcePath:      path,
				DestinationPath: destinationPath,
				Hash:            descriptor["hash"].(string),
			}
		}

		// initiate the transfer
		sourceEndpoint, err := endpoints.NewEndpoint(task.SourceEndpoint)
		if err != nil {
			return Task{}, err
		}
		destinationEndpoint, err := endpoints.NewEndpoint(task.DestinationEndpoint)
		if err != nil {
			return Task{}, err
		}
		transferId, err := sourceEndpoint.Transfer(destinationEndpoint, fileXfers)
		if err != nil {
			channels.Error <- err
			return Task{}, err
		}
		task.Status.TransferId = uuid.NullUUID{
			UUID:  transferId,
			Valid: true,
		}
		task.Status.TransferStatus = endpoints.TransferStatus{
			Code:     endpoints.TransferStatusActive,
			NumFiles: len(task.Descriptors),
		}
		task.Status.StagingId = uuid.NullUUID{}

		// wait for it to finish
		for task.Status.Code != TaskStatusSucceeded && task.Status.Code != TaskStatusFailed {
			time.Sleep(time.Duration(config.Service.PollInterval) * time.Millisecond)
			status, err := sourceEndpoint.Status(transferId)
			if err != nil {
				return Task{}, err
			}
			// FIXME: need to be more careful about this info vvv
			task.Status.TransferStatus = endpoints.TransferStatus{
				Code:     status.Code,
				NumFiles: status.NumFiles,
			}
		}

		return task, err
	}
	cancel := func(task Task, err error) {
		// send all errors to our error reporting channel
		channels.Error <- err
	}
	return pipeline.NewProcessor(process, cancel)
}

// manifest generation stage
func GenerateManifest(channels StageChannels) pipeline.Processor[Transfer, Transfer] {
	process := func(ctx context.Context, transfer Transfer) (Transfer, error) {
		localEndpoint, err := endpoints.NewEndpoint(config.Service.Endpoint)
		if err != nil {
			channels.Error <- err
			return Transfer{}, err
		}

		// generate a manifest for the transfer
		descriptors := make([]any, 0)
		for _, task := range transfer.Tasks {
			descriptors = append(descriptors, task.Descriptors...)
		}
		for _, dataDescriptor := range transfer.DataDescriptors {
			descriptors = append(descriptors, dataDescriptor)
		}

		transferUser := map[string]any{
			"title": transfer.Specification.User.Name,
			"role":  "author",
		}
		if transfer.Specification.User.Organization != "" {
			transferUser["organization"] = transfer.Specification.User.Organization
		}
		if transfer.Specification.User.Email != "" {
			transferUser["email"] = transfer.Specification.User.Email
		}

		descriptor := map[string]any{
			"name":      "manifest",
			"resources": descriptors,
			"created":   time.Now().Format(time.RFC3339),
			"profile":   "data-package",
			"keywords":  []any{"dts", "manifest"},
			"contributors": []any{
				transferUser,
			},
			"description":  transfer.Specification.Description,
			"instructions": transfer.Specification.Instructions,
		}

		manifest, err := datapackage.New(descriptor, ".")
		if err != nil {
			slog.Error(err.Error())
		}

		// write the manifest to disk and begin transferring it to the
		// destination endpoint
		transfer.ManifestFile = filepath.Join(config.Service.ManifestDirectory, fmt.Sprintf("manifest-%s.json", transfer.Id.String()))
		err = manifest.SaveDescriptor(transfer.ManifestFile)
		if err != nil {
			return Transfer{}, fmt.Errorf("creating manifest file: %s", err.Error())
		}

		// construct the source/destination file manifest paths
		fileXfers := []FileTransfer{
			{
				SourcePath:      transfer.ManifestFile,
				DestinationPath: filepath.Join(transfer.DestinationFolder, "manifest.json"),
			},
		}

		// begin transferring the manifest
		// FIXME: how do we determine the database's destination endpoint?
		destinationEndpointName := config.Databases[transfer.Specification.Destination].Endpoint
		destinationEndpoint, err := endpoints.NewEndpoint(destinationEndpointName)
		if err != nil {
			return Transfer{}, err
		}
		transfer.Status.ManifestId.UUID, err = localEndpoint.Transfer(destinationEndpoint, fileXfers)
		if err != nil {
			return Transfer{}, fmt.Errorf("transferring manifest file: %s", err.Error())
		}

		transfer.Status.Code = TransferStatusFinalizing
		transfer.Status.ManifestId.Valid = true
		return transfer, nil
	}
	cancel := func(transfer Transfer, err error) {
		// send all errors to our error reporting channel
		channels.Error <- err
	}
	return pipeline.NewProcessor(process, cancel)
}

// computes the size of a payload for a transfer (in Gigabytes)
func payloadSize(descriptors []any) float64 {
	var size uint64
	for _, d := range descriptors {
		descriptor := d.(map[string]any)
		size += uint64(descriptor["bytes"].(int))
	}
	return float64(size) / float64(1024*1024*1024)
}
