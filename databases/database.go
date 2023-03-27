package databases

import (
	"fmt"

	"github.com/google/uuid"

	"dts/core"
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
	Files []core.File `json:"files"`
}

// Database defines the interface for a database that is used to search for
// files and initiate file transfers
type Database interface {
	// search for files using the given parameters
	Search(params SearchParameters) (SearchResults, error)
	// returns true if the files identified by IDs are present in the database's
	// staging area AND are valid, false if not
	FilesStaged(fileIds []string) (bool, error)
	// begins staging the files for a transfer, returning a UUID representing the
	// staging operation
	StageFiles(fileIds []string) (uuid.UUID, error)
}

// creates a database based on the configured type
func NewDatabase(dbName string) (Database, error) {
	if dbName == "jdp" {
		return NewJdpDatabase(dbName)
	} else {
		return nil, fmt.Errorf("Unknown database type for '%s'", dbName)
	}
}
