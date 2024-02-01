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

package kbase

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/frictionless"
)

// file database appropriate for handling KBase searches and transfers
// (implements the databases.Database interface)
type Database struct {
	// database identifier
	Id string
}

func NewDatabase(orcid string) (databases.Database, error) {
	if orcid == "" {
		return nil, fmt.Errorf("No ORCID ID was given")
	}

	return &Database{
		Id: "kbase",
	}, nil
}

func (db *Database) Search(params databases.SearchParameters) (databases.SearchResults, error) {
	err := fmt.Errorf("Search not implemented for kbase database!")
	return databases.SearchResults{}, err
}

func (db *Database) Resources(fileIds []string) ([]frictionless.DataResource, error) {
	err := fmt.Errorf("Resources not implemented for kbase database!")
	return nil, err
}

func (db *Database) StageFiles(fileIds []string) (uuid.UUID, error) {
	err := fmt.Errorf("StageFiles not implemented for kbase database!")
	return uuid.UUID{}, err
}

func (db *Database) StagingStatus(id uuid.UUID) (databases.StagingStatus, error) {
	err := fmt.Errorf("StagingStatus not implemented for kbase database!")
	return databases.StagingStatusUnknown, err
}

func (db *Database) Endpoint() (endpoints.Endpoint, error) {
	return endpoints.NewEndpoint(config.Databases[db.Id].Endpoint)
}

// FIXME: currently we store a mapping of orcid IDs -> KBase user names
// FIXME: in a file called "kbase_users.json" in the DTS's working dir.
var kbaseUsers map[string]string

func (db *Database) LocalUser(orcid string) (string, error) {
	if kbaseUsers == nil {
		data, err := os.ReadFile("kbase_users.json")
		if err == nil {
			err = json.Unmarshal(data, &kbaseUsers)
		} else {
			// make an empty map to ѕignify the absence of the file
			kbaseUsers = make(map[string]string)
		}
	}
	if len(kbaseUsers) > 0 {
		username, found := kbaseUsers[orcid]
		if !found {
			return "", fmt.Errorf("No KBase user found for ORCID %s", orcid)
		} else {
			return username, nil
		}
	} else {
		// no current mechanism for this
		return "localuser", nil
	}
}
