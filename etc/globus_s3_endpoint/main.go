// Copyright (c) 2026 The KBase Project and its Contributors
// Copyright (c) 2026 Cohere Consulting, LLC
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

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/kbase/dts/etc/globus_s3_endpoint/globus_s3_api"
)

const (
	defaultPort = "8080"
)

var globusEndpointIds = []uuid.UUID{
	uuid.MustParse("8409a10b-de09-4670-a886-2c0b33f0fe25"), // ESnet Sunnyvale DTN (read-only test endpoint)
}

var globusEndpoints map[uuid.UUID]globus_s3_api.Endpoint

func main() {
	// set up Globus endpoints
	globusEndpoints = make(map[uuid.UUID]globus_s3_api.Endpoint)
	for _, endpointId := range globusEndpointIds {
		config, err := getGlobusConfig(endpointId)
		if err != nil {
			log.Fatalf("Error getting Globus config for endpoint %s: %s\n", endpointId.String(), err.Error())
		}
		endpoint, err := globus_s3_api.NewEndpoint(config)
		if err != nil {
			log.Fatalf("Error creating Globus endpoint %s: %s\n", endpointId.String(), err.Error())
		}
		globusEndpoints[endpointId] = endpoint
		log.Printf("Successfully set up Globus endpoint %s\n", endpointId.String())
	}

	// set up HTTP server with Gorilla Mux router
	r := mux.NewRouter()

	// handle root requests
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, Globus S3 Endpoint!\n")
		fmt.Fprintf(w, "Use /{bucket}/{path} to access buckets and objects.\n")
		fmt.Fprintf(w, "Configured Globus Endpoints:\n")
		for id := range globusEndpoints {
			fmt.Fprintf(w, "- %s\n", id.String())
		}
	})

	// handle bucket and object requests
	r.HandleFunc("/{bucket}/{path:.*}", handleBucketObjectRequest)

	port := ":" + defaultPort
	log.Println("Starting server on", port)
	if err := http.ListenAndServe(port, r); err != nil {
		log.Fatalf("Could not start server: %s\n", err.Error())
	}
}

func getGlobusConfig(endpointId uuid.UUID) (globus_s3_api.Config, error) {
	if endpointId == uuid.Nil {
		return globus_s3_api.Config{}, fmt.Errorf("invalid DTS_GLOBUS_ENDPOINT_ID: %s", endpointId.String())
	}
    clientId, err := uuid.Parse(os.Getenv("DTS_GLOBUS_CLIENT_ID"))
	if err != nil {
		return globus_s3_api.Config{}, fmt.Errorf("invalid DTS_GLOBUS_CLIENT_ID: %s", err.Error())
	}
	clientSecret := os.Getenv("DTS_GLOBUS_CLIENT_SECRET")
	if clientSecret == "" {
		return globus_s3_api.Config{}, fmt.Errorf("DTS_GLOBUS_CLIENT_SECRET environment variable not set")
	}
	config := globus_s3_api.Config{
		EndpointID:   endpointId,
		ClientId:     clientId,
		ClientSecret: clientSecret,
	}
	return config, nil
}

// handleBucketObjectRequest handles requests for buckets and objects
//
// Example requests:
//   GET /my-bucket
//   GET /my-bucket/path/to/object.txt
func handleBucketObjectRequest(w http.ResponseWriter, r *http.Request) {
	// parse bucket and object from URL
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	path := vars["path"]

	// handle request
	fmt.Fprintf(w, "Trying to retrieve Bucket: %s\nPath: /%s\n", bucket, path)
	if endpoint, ok := globusEndpoints[globusEndpointIds[0]]; ok {
		data, err := endpoint.HandleGetRequest(path)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error retrieving data from Globus endpoint: %s", err.Error()), http.StatusInternalServerError)
			return
		}
		// extract file names from json response
        // Try to parse as directory listing
        var response struct {
        	DATA []struct {
                Name         string `json:"name"`
                Type         string `json:"type"`
                Size         int64  `json:"size"`
                LastModified string `json:"last_modified"`
            } `json:"DATA"`
        	DataType string `json:"DATA_TYPE"`
    	}
		err = json.Unmarshal(data, &response)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error parsing JSON response: %s", err.Error()), http.StatusInternalServerError)
			return
		}
		for _, values := range response.DATA {
			fmt.Fprintf(w, " — %s\n", values.Name)
		}
	} else {
		http.Error(w, "Globus endpoint not found", http.StatusInternalServerError)
	}
}
