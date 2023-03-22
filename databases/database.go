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
// FIXME: for now, we use the structure defined by the JGI Data Portal, but
// FIXME: as we expand our scope this will likely change to a more generic
// FIXME: ElasticSearch setup (e.g. https://github.com/elastic/go-elasticsearch)
type SearchResults struct {
	Files []core.File `json:"files"`
}

// a file transfer, identified by a UUID
type Transfer struct {
	// unique identifier for transfer operation
	Id uuid.UUID `json:"id"`
}

// Database defines the interface for a database that is used to search for
// files and initiate file transfers
type Database interface {
	// search for files using the given parameters
	Search(params SearchParameters) (SearchResults, error)
	// returns true if the files identified by IDs are present in the database's
	// staging area AND are valid, false if not
	FilesStaged(fileIds []string) bool
	// begins staging the files for a transfer, returning a new Transfer
	StageFiles(fileIds []string) (Transfer, error)
}

// creates a database based on the configured type
func NewDatabase(dbName string) (Database, error) {
	if dbName == "jdp" {
		return NewJdpDatabase(dbName)
	} else {
		return nil, fmt.Errorf("Unknown database type for '%s'", dbName)
	}
}
