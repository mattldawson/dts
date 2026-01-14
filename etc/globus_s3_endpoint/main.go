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
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/google/uuid"
	"github.com/kbase/dts/etc/globus_s3_endpoint/globus_s3_api"
)

var globusEndpoint globus_s3_api.Endpoint

func main() {
	// set up Globus endpoint
	config, err := getGlobusConfig()
	if err != nil {
		log.Fatalf("Could not get Globus config: %s\n", err.Error())
	}

	globusEndpoint, err = globus_s3_api.NewEndpoint(config)
	if err != nil {
		log.Fatalf("Could not create Globus endpoint: %s\n", err.Error())
	}

	// set up HTTP server
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, Globus S3 Endpoint!\n\nDisable Verify? %v\nForce Verify? %v\n", globusEndpoint.Settings.DisableVerify, globusEndpoint.Settings.ForceVerify)
	})

	port := ":8080"
	log.Println("Starting server on", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Could not start server: %s\n", err.Error())
	}
}

func getGlobusConfig() (globus_s3_api.Config, error) {
	endpointId, err := uuid.Parse("8409a10b-de09-4670-a886-2c0b33f0fe25")
	if err != nil {
		return globus_s3_api.Config{}, fmt.Errorf("invalid DTS_GLOBUS_ENDPOINT_ID: %s", err.Error())
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


    
	
