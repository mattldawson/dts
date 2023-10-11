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
	"io"

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
	// provides a Close() method
	io.Closer
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
