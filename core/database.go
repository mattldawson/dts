package core

import (
	"github.com/google/uuid"
)

// parameters that define a search for files
type SearchParameters struct {
	// ElasticSearch query string
	Query string
	// pagination support
	Pagination struct {
		// number of search results to skip
		Offset int
		// maximum number of search results to include (0 indicates no max)
		MaxNum int
	}
}

// results from an ElasticSearch query
type SearchResults struct {
	Resources []DataResource `json:"resources"`
}

// This "enum" type identifies the status of a staging operation that moves
// files into place on a Database's endpoint.
type StagingStatus int

const (
	StagingStatusUnknown   StagingStatus = iota // unknown staging operation or status not available
	StagingStatusActive                         // staging in progress
	StagingStatusSucceeded                      // staging completed successfully
	StagingStatusFailed                         // staging failed
)

// Database defines the interface for a database that is used to search for
// files and initiate file transfers
type Database interface {
	// search for files using the given parameters
	Search(params SearchParameters) (SearchResults, error)
	// returns a slice of Frictionless DataResources for the files with the
	// given IDs
	Resources(fileIds []string) ([]DataResource, error)
	// begins staging the files for a transfer, returning a UUID representing the
	// staging operation
	StageFiles(fileIds []string) (uuid.UUID, error)
	// returns the status of a given staging operation
	StagingStatus(id uuid.UUID) (StagingStatus, error)
	// returns the endpoint associated with this database
	Endpoint() Endpoint
}
