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
	"fmt"

	"dts/core"
	"dts/databases/jdp"
)

// we maintain a table of database instances, identified by their names
var allDatabases map[string]core.Database = make(map[string]core.Database)

// creates a database proxy associated with the given ORCID ID, based on the
// configured type, or returns an existing instance
func NewDatabase(orcid, dbName string) (core.Database, error) {
	var err error

	// do we have one of these already?
	key := fmt.Sprintf("orcid: %s db: %s", orcid, dbName)
	db, found := allDatabases[key]
	if !found {
		// go get one
		if dbName == "jdp" {
			db, err = jdp.NewDatabase(orcid, dbName)
		} else {
			err = fmt.Errorf("Unknown database type for '%s'", dbName)
		}
		if err == nil {
			allDatabases[dbName] = db // stash it
		}
	}
	return db, err
}
