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

package config

// A database provides files for a file transfer (at its source or destination).
type databaseConfig struct {
	// the full name of the database
	Name string `yaml:"name"`
	// the name of the organization hosting the database
	Organization string `yaml:"organization"`
	// the base URL at which the database is accessed
	URL string `yaml:"url"`
	// the name of an endpoint for this database
	Endpoint string `yaml:"endpoint"`
	// authorization data (client secret passed in headers to authorize requests)
	Auth authConfig `yaml:"auth"`
	// the name of a monitoring service used by this database for notifications
	Notifications string `yaml:"notifications"`
	// search instructions
	Search struct {
		// ElasticSearch parameters
		ElasticSearch struct {
			// the resource for ElasticSearch queries (GET)
			Resource string `yaml:"resource"`
			// the parameter to which an ES query string is passed
			QueryParameter string `yaml:"query_parameter"`
		} `yaml:"elasticsearch"`
	} `yaml:"search"`
	// transfer initiation instructions
	Initiate struct {
		// transfer initiation resource (POST)
		Resource string `yaml:"resource"`
		// transfer initiation request body fields
		Request map[string]string `yaml:"request"`
	} `yaml:"initiate"`
	// file inspection instructions
	Inspect struct {
		// file inspection resource (POST)
		Resource string `yaml:"resource"`
		// file inspection request body fields
		Request map[string]string `yaml:"request"`
	} `yaml:"inspect"`
}
