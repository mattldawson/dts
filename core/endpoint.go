package core

import (
	"github.com/google/uuid"
)

// this type holds all relevant information for the transfer of an individual
// file
type FileTransfer struct {
	// absolute source and destination paths on respective endpoints
	SourcePath, DestinationPath string
	// MD5 checksum for the file
	MD5Checksum string
}

// this "enum" type encodes the status of a file transfer between endpoints
type TransferStatusCode int

const (
	TransferStatusUnknown   TransferStatusCode = iota
	TransferStatusActive                       // transfer in progress
	TransferStatusInactive                     // transfer suspended
	TransferStatusSucceeded                    // transfer completed successfully
	TransferStatusFailed                       // transfer failed
)

// this type conveys various information about a file transfer's status
type TransferStatus struct {
	// status code (see above)
	StatusCode TransferStatusCode
	// total number of files being transferred
	NumFiles int
	// number of files that have been transferred
	NumFilesTransferred int
}

// This type represents an endpoint for transferring files.
type Endpoint interface {
	// returns true if the files with the given absolute paths are staged at this
	// endpoint AND are valid, false otherwise
	FilesStaged(filePaths []string) (bool, error)
	// returns a list of UUIDs for all transfers associated with this endpoint
	Transfers() ([]uuid.UUID, error)
	// begins a transfer task that moves the files identified by the FileTransfer
	// structs, returning a UUID that can be used to refer to this task.
	Transfer(dst Endpoint, files []FileTransfer) (uuid.UUID, error)
	// retrieves the status for a transfer task identified by its UUID
	Status(id uuid.UUID) (TransferStatus, error)
	// cancels the transfer task with the given UUID
	Cancel(id uuid.UUID) error
}
