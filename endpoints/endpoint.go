package endpoints

import (
	"github.com/google/uuid"

	"dts/config"
)

// This "enum" type identifies the status of a file transfer operation.
type TransferStatus int

const (
	Unknown   TransferStatus = iota // unknown transfer or status not available
	Active                          // transfer in progress
	Inactive                        // transfer suspended
	Succeeded                       // transfer completed successfully
	Failed                          // transfer failed
)

// This type represents an endpoint for transferring files.
type Endpoint interface {
	// returns true if the files with the given absolute paths are staged at this
	// endpoint AND are valid, false otherwise
	FilesStaged(filePaths []string) (bool, error)
	// returns a list of UUIDs for all transfers associated with this endpoint
	Transfers() ([]uuid.UUID, error)
	// begins a transfer task that moves the files with the given absolute "src"
	// paths to their respective "dst" paths on the destination endpoint,
	// returning a UUID that can be used to refer to this task.
	Transfer(dst Endpoint, srcPaths, dstPaths []string) (uuid.UUID, error)
	// retrieves the status for a transfer task identified by its UUID
	Status(id uuid.UUID) (TransferStatus, error)
	// cancels the transfer task with the given UUID
	Cancel(uuid.UUID) error
}

// creates an endpoint based on the configured type
func NewEndpoint(endpointName string) (Endpoint, error) {
	epConfig := config.Endpoints[endpointName]
	if len(epConfig.Globus.URL) > 0 {
		return NewGlobusEndpoint(endpointName)
	}
	return nil, nil
}
