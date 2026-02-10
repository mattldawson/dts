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
	defaultDomain = "localhost"
	defaultPort = "8080"
)

var defaultOauthRedirectUri = fmt.Sprintf("http://%s:%s/oauth/callback", defaultDomain, defaultPort)

var globusEndpointIds = []uuid.UUID{
//	uuid.MustParse("8409a10b-de09-4670-a886-2c0b33f0fe25"), // ESnet Sunnyvale DTN (read-only test endpoint)
	uuid.MustParse(getGlobusTestEndpointId()),              // User-specified test endpoint
}

var globusEndpoints map[uuid.UUID]globus_s3_api.Endpoint

func getGlobusTestEndpointId() string {
	endpointId := os.Getenv("DTS_GLOBUS_TEST_ENDPOINT")
	if endpointId == "" {
		log.Fatal("DTS_GLOBUS_TEST_ENDPOINT environment variable not set")
	}
	return endpointId
}

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

	// OAuth callback handler
	r.HandleFunc("/{bucket}/oauth/callback", handleOAuthCallback).Methods(http.MethodGet)

    // Authorization URL helpder endpoint
	r.HandleFunc("/{bucket}/oauth/authorize", handleGetAuthorizationUrl).Methods(http.MethodGet)

	// handle root requests (list buckets)
	r.HandleFunc("/", handleRootRequest).Methods(http.MethodGet)

    // handle list bucket objects requests
	r.HandleFunc("/{bucket}", handleBucketRequest).Methods(http.MethodGet)

	// handle GetObject requests
	r.HandleFunc("/{bucket}/{path:.*}", handleGetObjectRequest).Methods(http.MethodGet)

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
		RedirectUri: fmt.Sprintf("http://%s:%s/%s/oauth/callback", defaultDomain, defaultPort, endpointId.String()),
	}
	return config, nil
}

// handleGetAuthorizationUrl handles requests to get the OAuth authorization URL for a bucket
func handleGetAuthorizationUrl(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	log.Printf("Getting authorization URL for bucket: %s\n", bucket)

	if bucket == "" {
		http.Error(w, "Bucket not specified in authorization URL request", http.StatusBadRequest)
		return
	}
	bucketUuid, err := uuid.Parse(bucket)
	if err != nil {
		log.Printf("Invalid bucket ID: %s\n", err.Error())
		http.Error(w, fmt.Sprintf("Invalid bucket ID: %s", err.Error()), http.StatusBadRequest)
		return
	}
	endpoint, ok := globusEndpoints[bucketUuid]
	if !ok {
		log.Printf("Globus endpoint not found for specified bucket: %s\n", bucket)
		http.Error(w, "Globus endpoint not found for specified bucket", http.StatusNotFound)
		return
	}

	authUrl := endpoint.GetAuthorizationUrl()
	log.Printf("Authorization URL for bucket %s: %s\n", bucket, authUrl)

	// return authorization URL as HTML
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, `
		<html>
		<head><title>Globus Authorization URL</title></head>
		<body>
			<h1>Globus Authorization URL for Bucket %s</h1>
			<p>Please visit the following URL to authorize access:</p>
			<p><a href="%s" target="_blank">Authorize Access</a></p>
            <p>Or copy and paste this URL into your browser:</p>
			<p>%s</p>
		</body>
		</html>
	`, bucket, authUrl, authUrl)
}

// handleOAuthCallback handles the OAuth callback from Globus
func handleOAuthCallback(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	log.Printf("OAuth callback for bucket: %s\n", bucket)

	if bucket == "" {
		http.Error(w, "Bucket not specified in OAuth callback", http.StatusBadRequest)
		return
	}
	bucketUuid, err := uuid.Parse(bucket)
	if err != nil {
		log.Printf("Invalid bucket ID: %s\n", err.Error())
		http.Error(w, fmt.Sprintf("Invalid bucket ID: %s", err.Error()), http.StatusBadRequest)
		return
	}
	endpoint, ok := globusEndpoints[bucketUuid]
	if !ok {
		log.Printf("Globus endpoint not found for specified bucket: %s\n", bucket)
		http.Error(w, "Globus endpoint not found for specified bucket", http.StatusNotFound)
		return
	}

	// parse code from query parameters
	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "Missing code parameter", http.StatusBadRequest)
		return
	}

	// check for errors
	if errMsg := r.URL.Query().Get("error"); errMsg != "" {
		http.Error(w, fmt.Sprintf("OAuth error: %s", errMsg), http.StatusBadRequest)
		return
	}

    session, err := endpoint.ExchangeAuthorizationCode(code)
	if err != nil {
		log.Printf("Error exchanging authorization code: %s\n", err.Error())
		http.Error(w, fmt.Sprintf("Error exchanging authorization code: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	log.Printf("Successfully obtained access token for bucket: %s\n", bucket)
	log.Printf("Access Token: %s\n", session.AccessToken)
	log.Printf("Refresh Token: %s\n", session.RefreshToken)

	// Replace existing endpoints with enpoints created with user tokens
	globusEndpoints[bucketUuid] = endpoint.WithSession(session)

	// return success message
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, `
        <html>
		<head><title>Authorization Successful</title></head>
		<body>
			<h1>Authorization Successful</h1>
			<p>You have successfully authorized access to the Globus endpoint for bucket %s.</p>
			<p>Access Token Expires: %s</p>
			<p>You can now close this window and return to your application.</p>
		</body>
		</html>
	`, bucket, session.Expires.Format(time.RFC1123))
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
func handleListBucketObjectsRequest(w http.ResponseWriter, r *http.Request) {
	// parse bucket from URL
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	log.Printf("Listing objects in Bucket: %s\n", bucket)

	// This is equivalent to listing the root path of the bucket
	bucketUuid, err := uuid.Parse(bucket)
	if err != nil {
		log.Printf("Invalid bucket ID: %s\n", err.Error())
		http.Error(w, fmt.Sprintf("Invalid bucket ID: %s", err.Error()), http.StatusBadRequest)
		return
	}
	endpoint, ok := globusEndpoints[bucketUuid]
	if !ok {
		log.Printf("Globus endpoint not found for specified bucket: %s\n", bucket)
		http.Error(w, "Globus endpoint not found for specified bucket", http.StatusNotFound)
		return
	}
	handleListObjectsV2Request(w, r, endpoint, bucket, "/")
}

// handlePathRequest handles requests for specific paths within a bucket
//
// Determines if the request is for listing contents of a bucket or retrieving a specific object
func handleBucketRequest(w http.ResponseWriter, r *http.Request) {
	// parse bucket and path from URL
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	log.Printf("handleBucketRequest: Bucket: %s\n", bucket)

	if bucket == "" {
		log.Printf("Bucket not specified in path request\n")
		http.Error(w, "Bucket not specified", http.StatusBadRequest)
		return
	}
	bucketUuid, err := uuid.Parse(bucket)
	if err != nil {
		log.Printf("Invalid bucket ID: %s\n", err.Error())
		http.Error(w, fmt.Sprintf("Invalid bucket ID: %s", err.Error()), http.StatusBadRequest)
		return
	}
	if _, ok := globusEndpoints[bucketUuid]; !ok {
		log.Printf("Globus endpoint not found for specified bucket: %s\n", bucket)
		http.Error(w, "Globus endpoint not found for specified bucket", http.StatusNotFound)
		return
	}

	// get the Globus endpoint for the specified bucket
	endpoint, ok := globusEndpoints[bucketUuid]
	if !ok {
		log.Printf("Globus endpoint not found for specified bucket: %s\n", bucket)
		http.Error(w, "Globus endpoint not found for specified bucket", http.StatusNotFound)
		return
	}

	// Get a prefix if one is specified
	path := r.URL.Query().Get("prefix")
	log.Printf("handleBucketRequest: Path: /%s\n", path)

	// Check the x-id query parameter to determine the operation
	listType := r.URL.Query().Get("list-type")
	log.Printf("Request list-type: %s\n", listType)

	switch listType {
	case "2":
		log.Printf("Handling as ListObjectsV2 request for bucket: %s path: /%s\n", bucket, path)
		handleListObjectsV2Request(w, r, endpoint, bucket, path)
	default:
		log.Printf("Unknown list-type (%s), for bucket: %s path: /%s\n", listType, bucket, path)
		log.Printf("Full header contents for unknown request:\n%s", r.Header)
		http.Error(w, fmt.Sprintf("Unknown list-type: %s", listType), http.StatusBadRequest)
	}
}

// handleGetObjectRequest handles requests to get a specific object from a bucket
func handleGetObjectRequest(w http.ResponseWriter, r *http.Request) {

	// parse bucket and path from URL
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	path := vars["path"]

	log.Printf("handleGetObjectRequest: Bucket: %s Path: /%s\n", bucket, path)

	if bucket == "" {
		log.Printf("Bucket not specified in GetObject request\n")
		http.Error(w, "Bucket not specified", http.StatusBadRequest)
		return
	}
	bucketUuid, err := uuid.Parse(bucket)
	if err != nil {
		log.Printf("Invalid bucket ID: %s\n", err.Error())
		http.Error(w, fmt.Sprintf("Invalid bucket ID: %s", err.Error()), http.StatusBadRequest)
		return
	}
	endpoint, ok := globusEndpoints[bucketUuid]
	if !ok {
		log.Printf("Globus endpoint not found for specified bucket: %s\n", bucket)
		http.Error(w, "Globus endpoint not found for specified bucket", http.StatusNotFound)
		return
	}

	// Try to get HTTPS URL for the object
	if endpoint.SupportsHttps() {
		log.Printf("Endpoint supports HTTPS access, attempting direct file access\n")
		handleDirectFileAccess(w, r, endpoint, path)
		return
	}

	log.Printf("Endpoint does not support HTTPS access, attempting HTTP get request\n")
	data, err := endpoint.HandleGetRequest(path)
	if err != nil {
		log.Printf("Error retrieving data from Globus endpoint: %s\n", err.Error())
		http.Error(w, fmt.Sprintf("Error retrieving data from Globus endpoint: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	fileName := getFileName(path)
	writeGetObjectResponse(w, r, data, fileName)
}

// handleDirectFileAccess handles direct file access via HTTPS URL from Globus
func handleDirectFileAccess(w http.ResponseWriter, r *http.Request, endpoint globus_s3_api.Endpoint, path string) {
	httpsUrl, err := endpoint.GetHttpsUrl(path)
	if err != nil {
		log.Printf("Error getting HTTPS URL from Globus endpoint: %s\n", err.Error())
		http.Error(w, fmt.Sprintf("Error getting HTTPS URL from Globus endpoint: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	// For S3 API compatability, we need to proxy the content through our server
	log.Printf("Proxying HTTPS content for path: /%s\n", path)
	endpoint.ProxyHttpsContent(w, r, httpsUrl, path)
}

// handleBucketObjectRequest handles requests for buckets and objects
//
// Example requests:
//   GET /my-bucket
//   GET /my-bucket/path/to/object.txt
func handleListObjectsV2Request(w http.ResponseWriter, r *http.Request, endpoint globus_s3_api.Endpoint, bucket string, path string) {

	// handle request
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
}

// writeGetObjectResponse writes the response for a GetObject request
func writeGetObjectResponse(w http.ResponseWriter, r *http.Request, data []byte, fileName string) {

	// Check if this is a range request
	rangeHeader := r.Header.Get("Range")
	log.Printf("Range request: %s\n", rangeHeader)
	if rangeHeader != "" {
		log.Printf("Handling range request: %s\n", rangeHeader)
		// Handle partial content (range) request
		handleRangeRequest(w, data, fileName, rangeHeader)
		return
	}

	log.Printf("Handling full content request for file: %s (size: %d bytes)\n", fileName, len(data))

	// write headers
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", fileName))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.Header().Set("ETag", fmt.Sprintf("\"%d\"", time.Now().UnixNano()))
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")

	// write status code
	w.WriteHeader(http.StatusOK)

	// write data to response
	w.Write(data)
}

// handleRangeRequest handles HTTP Range requests for partial content
func handleRangeRequest(w http.ResponseWriter, data []byte, fileName string, rangeHeader string) {
	var start, end int64
	totalSize := len(data)
	start, end, err := parseRange(rangeHeader, totalSize)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid Range header: %s", err.Error()), http.StatusRequestedRangeNotSatisfiable)
		return
	}
	
	// write headers for partial content
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", fileName))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
	w.Header().Set("ETag", fmt.Sprintf("\"%d\"", time.Now().UnixNano()))
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))

	// write status code for partial content
	w.WriteHeader(http.StatusPartialContent)
	
	// write partial data to response
	w.Write(data[start : end+1])
}

// parseRange parses the Range header and returns the start and end byte positions
func parseRange(rangeHeader string, totalSize int) (int64, int64, error) {
	var start, end int64
	_, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)
	if err != nil {
		if _, err = fmt.Sscanf(rangeHeader, "bytes=%d-", &start); err == nil {
			end = int64(totalSize) - 1
			return start, end, nil
		}
		return 0, 0, fmt.Errorf("invalid Range header format")
	}
	if end >= int64(totalSize) || end == 0 {
		end = int64(totalSize) - 1
	}
	if start < 0 || start > end {
		return 0, 0, fmt.Errorf("invalid byte range")
	}
	return start, end, nil
}

// loggingMiddleware logs incoming requests and outgoing responses
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

// getFileName extracts the file name from a given path
func getFileName(path string) string {
	lastSlash := -1
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			lastSlash = i
			break
		}
	}
	if lastSlash == -1 {
		return path
	}
	return path[lastSlash+1:]
}
