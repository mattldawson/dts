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

package journal

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/frictionlessdata/datapackage-go/validator"
	"github.com/google/uuid"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/kbase/dts/config"
)

// This is the DTS transfer journal, which logs all transfer activity. The journal is a table of
// transfer records (one per transfer).

// a record storing all information relevant to a transfer
type Record struct {
	// UUID associated with the transfer
	Id uuid.UUID
	// the source and destination associated with the transfer
	Source, Destination string
	// the ORCID associated with the transfer
	Orcid string
	// times at which the transfer was requested and at which it completed
	StartTime, StopTime time.Time
	// status of the transfer ("succeeded", "failed", or "canceled")
	Status string
	// size of the transfer's payload in bytes
	PayloadSize int64
	// number of files in the transfer's payload
	NumFiles int
	// manifest containing metadata for the transfer's payload
	Manifest *datapackage.Package
}

// initialize the DTS transfer journal
func Init() error {
	if !IsOpen() {
		var err error
		dbPath := filepath.Join(config.Service.DataDirectory, fmt.Sprintf("%s-journal.db", config.Service.Name))
		conn_, err = sqlite.OpenConn(dbPath)
		if err != nil {
			return &CantOpenError{
				Message: err.Error(),
			}
		}
		return createSchema()
	}
	return nil
}

// saves and closes the DTS transfer journal (if it's been opened)
func Finalize() error {
	if conn_ != nil {
		err := conn_.Close()
		conn_ = nil
		if err != nil {
			return &CantCloseError{
				Message: err.Error(),
			}
		}
	}
	return nil
}

// returns true if the journal is open for writing, false if not
func IsOpen() bool {
	return conn_ != nil
}

// records a completed transfer
// record: the record containing all transfer information
// manifestFile: the path to the manifest file (to be read and included in the journal)
func RecordTransfer(record Record) error {
	switch record.Status {
	case "succeeded", "failed", "canceled":
	default:
		return &NewRecordError{
			Id:      record.Id,
			Message: fmt.Sprintf("Invalid status: %s", record.Status),
		}
	}

	if !IsOpen() {
		return &NotOpenError{}
	}
	startTime := record.StartTime.Format(time.DateTime)
	stopTime := record.StopTime.Format(time.DateTime)
	err := sqlitex.Execute(conn_, "INSERT INTO transfers (id, source, destination, orcid, start_time, stop_time, status, payload_size, num_files) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
		&sqlitex.ExecOptions{
			Args: []any{record.Id.String(), record.Source, record.Destination, record.Orcid, startTime, stopTime, record.Status, record.PayloadSize, record.NumFiles},
		})
	if err != nil {
		return &NewRecordError{
			Id:      record.Id,
			Message: err.Error(),
		}
	}

	// if the transfer succeeded, store its manifest
	if record.Manifest != nil {
		jsonManifest, err := json.Marshal(record.Manifest.Descriptor())
		if err != nil {
			return &NewRecordError{
				Id:      record.Id,
				Message: err.Error(),
			}
		}
		return sqlitex.Execute(conn_, "INSERT INTO manifests (id, manifest) VALUES (?, JSON(?));",
			&sqlitex.ExecOptions{
				Args: []any{record.Id.String(), string(jsonManifest)},
			})
	}
	return nil
}

// retrieves the transfer record for the given ID -- useful for testing
// id: the unique identifier for the transfer
func TransferRecord(id uuid.UUID) (Record, error) {
	if !IsOpen() {
		return Record{}, &NotOpenError{}
	}
	var record Record
	err := sqlitex.Execute(conn_, "SELECT source, destination, orcid, start_time, stop_time, status, payload_size, num_files FROM transfers WHERE id = ?;",
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				startTime, err := time.Parse(time.DateTime, stmt.ColumnText(3))
				if err != nil {
					return &InvalidRecordError{
						Id:      record.Id,
						Message: "bad start time",
					}
				}
				stopTime, err := time.Parse(time.DateTime, stmt.ColumnText(4))
				if err != nil {
					return &InvalidRecordError{
						Id:      record.Id,
						Message: "bad stop time",
					}
				}
				record = Record{
					Id:          id,
					Source:      stmt.ColumnText(0),
					Destination: stmt.ColumnText(1),
					Orcid:       stmt.ColumnText(2),
					StartTime:   startTime,
					StopTime:    stopTime,
					Status:      stmt.ColumnText(5),
					PayloadSize: stmt.ColumnInt64(6),
					NumFiles:    stmt.ColumnInt(7),
				}
				return nil
			},
			Args: []any{id.String()},
		})

	// get the manifest if possible
	if record.Status == "succeeded" {
		err = sqlitex.Execute(conn_, "SELECT manifest FROM manifests WHERE id = ?;",
			&sqlitex.ExecOptions{
				ResultFunc: func(stmt *sqlite.Stmt) error {
					manifest, err := datapackage.FromString(stmt.ColumnText(0), "manifest.json", validator.InMemoryLoader())
					if err != nil {
						return &InvalidRecordError{
							Id:      record.Id,
							Message: "unable to retrieve manifest for successful transfer",
						}
					}
					record.Manifest = manifest
					return nil
				},
				Args: []any{id.String()},
			})
	}
	return record, err
}

//-----------
// Internals
//-----------

// transfer journal database connection
var conn_ *sqlite.Conn

// creates the schema for the transfer journal when it's first created
func createSchema() error {
	script :=
		`CREATE TABLE IF NOT EXISTS transfers (
	id TEXT PRIMARY KEY,
	source TEXT NOT NULL,
	destination TEXT NOT NULL,
	orcid TEXT NOT NULL,
	start_time TEXT NOT NULL,
	stop_time TEXT,
	status TEXT,
	payload_size INTEGER NOT NULL,
	num_files INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS manifests (
	id TEXT,
	manifest TEXT NOT NULL
);
`
	return sqlitex.ExecuteScript(conn_, script, nil)
}
