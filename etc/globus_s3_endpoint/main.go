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
	"time"

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

	// add logging middleware
	r.Use(loggingMiddleware)

	// handle root requests (list buckets)
	r.HandleFunc("/", handleRootRequest).Methods(http.MethodGet)

    // handle bucket requests (list objects)
	r.HandleFunc("/{bucket}", handleBucketRequest).Methods(http.MethodGet)

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

// handleRootRequest handles requests to the root URL (ListBuckets in S3 API)
func handleRootRequest(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Listing available Globus endpoints:")

	// Return XML response for S3 ListBuckets
	w.Header().Set("Content-Type", "application/xml")

	// Simple S3 ListBuckets XML response
	fmt.Fprintln(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Owner>
	    <ID>globus-owner</ID>
		<DisplayName>Globus S3</DisplayName>
	</Owner>
    <Buckets>
`)
    for id := range globusEndpoints {
		fmt.Fprintf(w, `        <Bucket>
	    <Name>%s</Name>
		<CreationDate>2024-01-01T00:00:00.000Z</CreationDate>
	</Bucket>
`, id.String())
	}

	fmt.Fprintln(w, `    </Buckets>
</ListAllMyBucketsResult>`)
}

// handleBucketRequest handles bucket-level requests (lists objects in the bucket)
func handleBucketRequest(w http.ResponseWriter, r *http.Request) {
	// parse bucket from URL
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	log.Printf("Listing objects in Bucket: %s\n", bucket)

	// This is equivalent to listing the root path of the bucket
	handleBucketObjectRequest(w, r)
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

	if path == "" {
		path = "/"
	}

	log.Printf("Retrieving object from Bucket: %s, Path: /%s\n", bucket, path)

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

		// Separate files and directories
		var files []struct {
			Name         string
			Size         int64
			LastModified string
		}
		var directories []string
		
		for _, item := range response.DATA {
			if item.Type == "dir" {
				directories = append(directories, item.Name)
			} else {
				files = append(files, struct {
					Name         string
					Size         int64
					LastModified string
				}{
					Name:         item.Name,
					Size:         item.Size,
					LastModified: item.LastModified,
				})
			}
		}

		log.Printf("Found %d files and %d directories\n", len(files), len(directories))
		
		// Return XML response for S3 ListObjectsV2
		w.Header().Set("Content-Type", "application/xml")

		// Simple S3 ListObjectsV2 XML response
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Name>%s</Name>
	<Prefix>%s</Prefix>
	<KeyCount>%d</KeyCount>
	<MaxKeys>1000</MaxKeys>
	<IsTruncated>false</IsTruncated>
`, bucket, path, len(response.DATA))
        // Write object entries
		for _, item := range response.DATA {
			if item.Type == "dir" {
				// skip directories for now
				continue
			}

			// Convert LastModified to ISO8601 format
			isoLastModified := convertToISO8601(item.LastModified)

			// Write object entry
			fmt.Fprintf(w, `    <Contents>
		<Key>%s</Key>
		<LastModified>%s</LastModified>
		<ETag>"%s"</ETag>
		<Size>%d</Size>
		<StorageClass>STANDARD</StorageClass>
	</Contents>
`, item.Name, isoLastModified, "dummy-etag", item.Size)
		}
        // Write directory entries as CommonPrefixes
		for _, dir := range directories {
			fmt.Fprintf(w, `    <CommonPrefixes>
		<Prefix>%s/</Prefix>
	</CommonPrefixes>
`, dir)
		}
		fmt.Fprintln(w, `</ListBucketResult>`)
	} else {
		http.Error(w, "Globus endpoint not found", http.StatusInternalServerError)
	}
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("→ [%s] %s %s from %s\n", r.Method, r.URL.Path, r.Proto, r.RemoteAddr)
		next.ServeHTTP(w, r)
		log.Printf("← [%s] %s - %d\n", r.Method, r.RequestURI, http.StatusOK)
	})
}

// convertToISO8601 converts Globus timestamp format to ISO8601/RFC3339 format
// Input: "2022-11-16 01:34:57+00:00"
// Output: "2022-11-16T01:34:57.000Z"
func convertToISO8601(timestamp string) string {
    // Parse the Globus timestamp format
    t, err := time.Parse("2006-01-02 15:04:05-07:00", timestamp)
    if err != nil {
        log.Printf("Error parsing timestamp %s: %s", timestamp, err.Error())
        // Return a default timestamp if parsing fails
        return time.Now().UTC().Format(time.RFC3339)
    }
    
    // Format to ISO8601/RFC3339 format
    return t.UTC().Format("2006-01-02T15:04:05.000Z")
}