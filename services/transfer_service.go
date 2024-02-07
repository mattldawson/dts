package services

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	"github.com/kbase/dts/frictionless"
)

// This package-specific helper function writes a JSON payload to an
// http.ResponseWriter.
func writeJson(w http.ResponseWriter, data []byte, code int) {
	w.WriteHeader(code)
	if len(data) > 0 {
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}
}

// This type holds information about an error that occurred responding to a
// request.
type ErrorResponse struct {
	// An HTTP error code
	Code int `json:"code"`
	// A descriptive error message
	Error string `json:"message"`
}

// This package-specific helper function writes an error to an
// http.ResponseWriter, giving it the proper status code, and encoding an
// ErrorResponse in the response body.
func writeError(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	e := ErrorResponse{Code: code, Error: message}
	data, _ := json.Marshal(e)
	w.Write(data)
}

// a response for an ElasticSearch query (GET)
type ElasticSearchResponse struct {
	// name of organization database
	Database string `json:"database"`
	// ElasticSearch query string
	Query string `json:"query"`
	// Resources matching the query
	Resources []frictionless.DataResource `json:"resources"`
}

// a request for a file transfer (POST)
type TransferRequest struct {
	// name of source database
	Source string `json:"source"`
	// identifiers for files to be transferred
	FileIds []string `json:"file_ids"`
	// name of destination database
	Destination string `json:"destination"`
	// ORCID identifier associated with the request
	Orcid string `json:"orcid"`
}

// a response for a file transfer request (POST)
type TransferResponse struct {
	// transfer job ID
	Id uuid.UUID `json:"id"`
}

// a response for a file transfer status request (GET)
type TransferStatusResponse struct {
	// transfer job ID
	Id string `json:"id"`
	// transfer job status
	Status string `json:"status"`
	// message (if any) related to status
	Message string `json:"message,omitempty"`
	// number of files being transferred
	NumFiles int `json:"num_files"`
	// number of files that have been completely transferred
	NumFilesTransferred int `json:"num_files_transferred"`
}

// TransferService defines the interface for our data transfer service.
type TransferService interface {
	// Starts the service on the selected port, returning an error that indicates
	// success or failure.
	Start(port int) error
	// Gracefully shuts down the service without interrupting active connections.
	Shutdown(ctx context.Context) error
	// Closes down the service, freeing all resources.
	Close()
}
