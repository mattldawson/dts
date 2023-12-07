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

package endpoints

import (
	"fmt"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/core"
	"github.com/kbase/dts/endpoints/globus"
)

// we maintain a table of endpoint instances, identified by their names
var allEndpoints map[string]core.Endpoint = make(map[string]core.Endpoint)

// creates an endpoint based on the configured type, or returns an existing
// instance
func NewEndpoint(endpointName string) (core.Endpoint, error) {
	var err error

	// do we have one of these already?
	endpoint, found := allEndpoints[endpointName]
	if !found {
		// is it a Globus endpoint?
		if config.Endpoints[endpointName].Provider == "globus" {
			endpoint, err = globus.NewEndpoint(endpointName)
		} else {
			err = fmt.Errorf("Invalid provider for endpoint '%s': %s", endpointName,
				config.Endpoints[endpointName].Provider)
		}

		// stash it
		if err == nil {
			allEndpoints[endpointName] = endpoint
		}
	}
	return endpoint, err
}
