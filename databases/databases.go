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

	"github.com/kbase/dts/core"
	"github.com/kbase/dts/databases/jdp"
	"github.com/kbase/dts/databases/kbase"
)

// we maintain a table of database instances, identified by their names
var allDatabases = make(map[string]core.Database)

// here's a table of database creation functions
var createDatabaseFuncs = make(map[string]func(name string) (core.Database, error))

// registers a database creation function under the given database name
// to allow for e.g. test database implementations
func RegisterDatabase(dbName string, createDb func(orcid string) (core.Database, error)) error {
	if _, found := createDatabaseFuncs[dbName]; found {
		return fmt.Errorf("Cannot register database %s (already registered)", dbName)
	} else {
		createDatabaseFuncs[dbName] = createDb
		return nil
	}
}

var firstNewDatabaseCall = true

// creates a database proxy associated with the given ORCID ID, based on the
// configured type, or returns an existing instance
func NewDatabase(orcid, dbName string) (core.Database, error) {
	var err error

	// register our built-in databases if this is the first call to this function
	if firstNewDatabaseCall {
		RegisterDatabase("jdp", jdp.NewDatabase)
		RegisterDatabase("kbase", kbase.NewDatabase)
		firstNewDatabaseCall = false
	}

	// do we have one of these already?
	key := fmt.Sprintf("orcid: %s db: %s", orcid, dbName)
	db, found := allDatabases[key]
	if !found {
		// create the requested database
		if createDb, valid := createDatabaseFuncs[dbName]; valid {
			db, err = createDb(orcid)
		} else {
			err = fmt.Errorf("Unknown database type for '%s'", dbName)
		}
		if err == nil {
			allDatabases[dbName] = db // stash it
		}
	}
	return db, err
}
