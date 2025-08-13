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
		go transferJournalProcess()
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

// saves and closes the DTS transfer journal (if it's been opened)
func Finalize() error {
	if IsOpen() {
		channels_.Input.Shutdown <- struct{}{}
		closeChannels()
	}
	return nil
}

// returns true if the journal is open for writing, false if not
func IsOpen() bool {
	if channels_.Open { // has Init() been called?
		channels_.Input.CheckIfOpen <- struct{}{}
		select {
		case isOpen := <-channels_.Output.IsOpen:
			return isOpen
		case <-time.After(1 * time.Second): // after a second, we assume the goroutine has crashed
			closeChannels()
			return false
		}
	}
	return false
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

	channels_.Input.CreateRecord <- record
	return <-channels_.Output.Error
}

// retrieves the transfer record for the given ID -- useful for testing
// id: the unique identifier for the transfer
func TransferRecord(id uuid.UUID) (Record, error) {
	if !IsOpen() {
		return Record{}, &NotOpenError{}
	}
	channels_.Input.FetchRecord <- id
	var record Record
	var err error
	select {
	case record := <-channels_.Output.Record:
		return record, err
	case err := <-channels_.Output.Error:
		return record, err
	}
}

//-----------
// Internals
//-----------

// The SQLite database gets its own goroutine so it doesn't bring down the entire service if it
// crashes. Here we define "input" channels (main process -> goroutine) and "output" channels
// (goroutine -> main process) for passing data back and forth

var channels_ struct {
	Open  bool // true if channels are open, false if not
	Input struct {
		CreateRecord chan Record    // for creating new records
		CheckIfOpen  chan struct{}  // for checking to see whether the database is open
		FetchRecord  chan uuid.UUID // for fetching records
		Shutdown     chan struct{}  // for shutting down the database
	}

	Output struct {
		Record chan Record // for returning records
		Error  chan error  // for returning errors
		IsOpen chan bool   // for answering queries about whether the database is open
	}
}

func transferJournalProcess() {

	openChannels()

	// open the database, creating the schema if necessary
	dbPath := filepath.Join(config.Service.DataDirectory, fmt.Sprintf("%s-journal.db", config.Service.Name))
	conn, err := sqlite.OpenConn(dbPath)
	if err != nil {
		channels_.Output.Error <- &CantOpenError{
			Message: err.Error(),
		}
	}
	err = createSchema(conn)
	if err != nil {
		channels_.Output.Error <- &CantOpenError{
			Message: err.Error(),
		}
	}

	// handle requests
	running := true
	for running {
		select {

		case <-channels_.Input.CheckIfOpen:
			channels_.Output.IsOpen <- true // always true if this goroutine is running!

		case record := <-channels_.Input.CreateRecord:
			err := createRecord(conn, record)
			channels_.Output.Error <- err

		case id := <-channels_.Input.FetchRecord:
			record, err := fetchRecord(conn, id)
			if err != nil {
				channels_.Output.Error <- err
			} else {
				channels_.Output.Record <- record
			}

		case <-channels_.Input.Shutdown:
			err := conn.Close()
			conn = nil
			if err != nil {
				channels_.Output.Error <- &CantCloseError{
					Message: err.Error(),
				}
			}
			running = false
		}
	}
}

func openChannels() {
	channels_.Open = true
	channels_.Input.CreateRecord = make(chan Record)
	channels_.Input.CheckIfOpen = make(chan struct{})
	channels_.Input.FetchRecord = make(chan uuid.UUID)
	channels_.Input.Shutdown = make(chan struct{})
	channels_.Output.Record = make(chan Record)
	channels_.Output.Error = make(chan error)
	channels_.Output.IsOpen = make(chan bool)
}

func closeChannels() {
	channels_.Open = false
	close(channels_.Input.CreateRecord)
	close(channels_.Input.CheckIfOpen)
	close(channels_.Input.FetchRecord)
	close(channels_.Input.Shutdown)
	close(channels_.Output.Record)
	close(channels_.Output.Error)
	close(channels_.Output.IsOpen)
}

// creates the schema for the transfer journal when it's first created
func createSchema(conn *sqlite.Conn) error {
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
	return sqlitex.ExecuteScript(conn, script, nil)
}

func createRecord(conn *sqlite.Conn, record Record) error {
	startTime := record.StartTime.Format(time.DateTime)
	stopTime := record.StopTime.Format(time.DateTime)
	err := sqlitex.Execute(conn, "INSERT INTO transfers (id, source, destination, orcid, start_time, stop_time, status, payload_size, num_files) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
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
		return sqlitex.Execute(conn, "INSERT INTO manifests (id, manifest) VALUES (?, JSON(?));",
			&sqlitex.ExecOptions{
				Args: []any{record.Id.String(), string(jsonManifest)},
			})
	}
	return nil
}

func fetchRecord(conn *sqlite.Conn, id uuid.UUID) (Record, error) {
	var record Record
	err := sqlitex.Execute(conn, "SELECT source, destination, orcid, start_time, stop_time, status, payload_size, num_files FROM transfers WHERE id = ?;",
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				startTime, err := time.Parse(time.DateTime, stmt.ColumnText(3))
				if err != nil {
					return &InvalidRecordError{
						Id:      id,
						Message: fmt.Sprintf("bad start time: %s", err.Error()),
					}
				}
				stopTime, err := time.Parse(time.DateTime, stmt.ColumnText(4))
				if err != nil {
					return &InvalidRecordError{
						Id:      id,
						Message: fmt.Sprintf("bad stop time: %s", err.Error()),
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
	if err != nil {
		return record, err
	}

	// get the manifest if possible
	if record.Status == "succeeded" {
		err = sqlitex.Execute(conn, "SELECT manifest FROM manifests WHERE id = ?;",
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
