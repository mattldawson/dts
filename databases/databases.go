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

package databases

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/kbase/dts/frictionless"
)

// Database defines the interface for a database that is used to search for
// files and initiate file transfers
type Database interface {
	// returns a mapping of database-specific search parameters to zeroed values
	// of specific types accessible via type switches
	// * supported types: int, string, bool, float64, slices
	// * slices represent sets of accepted values of their respective types
	//   (useful for pulldown menus)
	// * databases with no specific search parameters should return nil
	SpecificSearchParameters() map[string]interface{}
	// search for files using the given parameters
	Search(params SearchParameters) (SearchResults, error)
	// returns a slice of Frictionless DataResources for the files with the
	// given IDs
	Resources(fileIds []string) ([]frictionless.DataResource, error)
	// begins staging the files for a transfer, returning a UUID representing the
	// staging operation
	StageFiles(fileIds []string) (uuid.UUID, error)
	// returns the status of a given staging operation
	StagingStatus(id uuid.UUID) (StagingStatus, error)
	// returns the local username associated with the given Orcid ID
	LocalUser(orcid string) (string, error)
}

// parameters that define a search for files
type SearchParameters struct {
	// ElasticSearch query string
	Query string
	// file status, if requested
	Status SearchFileStatus
	// pagination support
	Pagination SearchPaginationParameters
	// database-specific search parameters with names matched to provided values
	// (validated by database)
	Specific map[string]json.RawMessage
}

// results from a file search
type SearchResults struct {
	Resources []frictionless.DataResource `json:"resources"`
}

type SearchPaginationParameters struct {
	// number of search results to skip
	Offset int
	// maximum number of search results to include (0 indicates no max)
	MaxNum int
}

// allows searching for files that are staged, not yet staged, etc
type SearchFileStatus int

const (
	SearchFileStatusAny SearchFileStatus = iota
	SearchFileStatusStaged
	SearchFileStatusUnstaged
)

// This enum identifies the status of a staging operation that moves
// files into place on a Database's endpoint.
type StagingStatus int

const (
	StagingStatusUnknown   StagingStatus = iota // unknown staging operation or status not available
	StagingStatusActive                         // staging in progress
	StagingStatusSucceeded                      // staging completed successfully
	StagingStatusFailed                         // staging failed
)

// registers a database creation function under the given database name
// to allow for e.g. test database implementations
func RegisterDatabase(dbName string, createDb func(orcid string) (Database, error)) error {
	if _, found := createDatabaseFuncs_[dbName]; found {
		return AlreadyRegisteredError{
			Database: dbName,
		}
	} else {
		createDatabaseFuncs_[dbName] = createDb
		return nil
	}
}

// creates a database proxy associated with the given ORCID ID, based on the
// configured type, or returns an existing instance
func NewDatabase(orcid, dbName string) (Database, error) {
	var err error

	// do we have one of these already?
	key := fmt.Sprintf("orcid: %s db: %s", orcid, dbName)
	db, found := allDatabases_[key]
	if !found {
		// create the requested database
		if createDb, valid := createDatabaseFuncs_[dbName]; valid {
			db, err = createDb(orcid)
		} else {
			err = NotFoundError{dbName}
		}
		if err == nil {
			allDatabases_[key] = db // stash it
		}
	}
	return db, err
}

//-----------
// Internals
//-----------

// we maintain a table of database instances, identified by their names
var allDatabases_ = make(map[string]Database)

// a table of database creation functions
var createDatabaseFuncs_ = make(map[string]func(name string) (Database, error))
