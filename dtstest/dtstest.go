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

// This package contains testing utilities for the Data Transfer System.
package dtstest

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/frictionless"
)

// Enables DEBUG log messages for DTS's structured log (slog).
func EnableDebugLogging() {
	logLevel := new(slog.LevelVar)
	logLevel.Set(slog.LevelDebug)
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})
	slog.SetDefault(slog.New(h))
}

// Given a specified configuration that has been initialized, this function
// registers test fixtures for databases and endpoints that meet the following
// criteria:
// * a database contains the word "test" in its identifier (YAML key)
// * an endpoint contains the word "test" in its "provider" field
func RegisterTestFixturesFromConfig() error {
	// has config.Init() been called?
	if len(config.Endpoints) == 0 && len(config.Databases) == 0 {
		return fmt.Errorf(`No endpoints or databases were found in the configuration.
Did you call config.Init()?`)
	}

	// register endpoint fixtures
	for _, endpointName := range config.Endpoints {

	}

	// register database fixtures
	for _, databaseName := range config.Databases {

	}

	return nil
}

//------------------------
// Endpoint Test Fixtures
//------------------------

type transferInfo struct {
	Time   time.Time // transfer initiation time
	Status endpoints.TransferStatus
}

// This type implements an Endpoint test fixture
type Endpoint struct {
	Database         *Database     // database fixture attached to endpoint
	StagingDuration  time.Duration // time it takes to "stage files"
	TransferDuration time.Duration // time it takes to "transfer files"
	Xfers            map[uuid.UUID]transferInfo
}

// Registers an endpoint test fixture with the given name in the configuration,
// assigning it specific durations to simulate transfers in a manner appropriate
// to tests of interest, and assigning it the given set of Frictionless
// DataResources.
func RegisterEndpoint(endpointName string, stagingDuration time.Duration,
	transferDuration time.Duration, resources map[string]frictionless.DataResource) error {
	newEndpointFunc := func(name string) (endpoints.Endpoint, error) {
		return &Endpoint{
			Resources:        resources,
			StagingDuration:  stagingDuration,
			TransferDuration: transferDuration,
			Xfers:            make(map[uuid.UUID]transferInfo),
		}, nil
	}
	return endpoints.RegisterEndpoint(endpointName, newEndpointFunc)
}

func (ep *Endpoint) Root() string {
	root, _ := os.Getwd()
	return root
}

func (ep *Endpoint) FilesStaged(files []DataResource) (bool, error) {
	if ep.Database != nil {
		// are there any unrecognized files?
		for _, file := range files {
			if _, found := ep.Resources[file.Id]; !found {
				return false, fmt.Errorf("Unrecognized file: %s\n", file.Id)
			}
		}
		// the source endpoint should report true for the staged files as long
		// as the source database has had time to stage them
		for _, req := range ep.Database.Staging {
			if time.Now().Sub(req.Time) < ep.StagingDuration {
				return false, nil
			}
		}
	}
	return true, nil
}

func (ep *Endpoint) Transfers() ([]uuid.UUID, error) {
	xfers := make([]uuid.UUID, 0)
	for xferId, _ := range ep.Xfers {
		xfers = append(xfers, xferId)
	}
	return xfers, nil
}

func (ep *Endpoint) Transfer(dst Endpoint, files []FileTransfer) (uuid.UUID, error) {
	xferId := uuid.New()
	ep.Xfers[xferId] = transferInfo{
		Time: time.Now(),
		Status: endpoints.TransferStatus{
			Code:                endpoints.TransferStatusActive,
			NumFiles:            len(files),
			NumFilesTransferred: 0,
		},
	}
	return xferId, nil
}

func (ep *Endpoint) Status(id uuid.UUID) (TransferStatus, error) {
	if info, found := ep.Xfers[id]; found {
		if info.Status.Code != endpoints.TransferStatusSucceeded &&
			time.Now().Sub(info.Time) >= transferDuration { // update if needed
			info.Status.Code = TransferStatusSucceeded
			ep.Xfers[id] = info
		}
		return info.Status, nil
	} else {
		return endpoints.TransferStatus{}, fmt.Errorf("Invalid transfer ID: %s", id.String())
	}
}

func (ep *Endpoint) Cancel(id uuid.UUID) error {
	// not used (yet)
	return nil
}

//------------------------
// Database Test Fixtures
//------------------------

type stagingRequest struct {
	FileIds []string
	Time    time.Time
}

// This type implements a databases.Database test fixture
type Database struct {
	Endpt     endpoints.Endpoint
	Resources map[string]frictionless.DataResources
	Staging   map[uuid.UUID]stagingRequest
}

// Registers a database test fixture with the given name in the configuration,
// assigning it the given Frictionless DataResources.
func RegisterDatabase(databaseName string, resources map[string]frictionless.DataResource) error {
	newDatabaseFunc := func(orcid string) (databases.Database, error) {
		endpoint, err := endpoints.NewEndpoint(config.Databases[databaseName].Endpoint)
		if err != nil {
			return nil, err
		}
		db := Database{
			Endpt:     endpoint,
			Resources: resources,
			Staging:   make(map[uuid.UUID]stagingRequest),
		}
		db.Endpt.(*Endpoint).Database = &db
		return &db, nil
	}
	return databases.RegisterDatabase(databaseName, newDatabaseFunc)
}

func (db *Database) Search(params databases.SearchParameters) (databases.SearchResults, error) {
	// this method is unused, so we just need a placeholder
	return databases.SearchResults{}, nil
}

func (db *Database) Resources(fileIds []string) ([]DataResource, error) {
	resources := make([]DataResource, 0)
	var err error
	for _, fileId := range fileIds {
		if resource, found := db.Resources[fileId]; found {
			resources = append(resources, resource)
		} else {
			err = fmt.Errorf("Unrecognized File ID: %s", fileId)
			break
		}
	}
	return resources, err
}

func (db *Database) StageFiles(fileIds []string) (uuid.UUID, error) {
	id := uuid.New()
	db.Staging[id] = stagingRequest{
		FileIds: fileIds,
		Time:    time.Now(),
	}
	return id, nil
}

func (db *Database) StagingStatus(id uuid.UUID) (databases.StagingStatus, error) {
	if info, found := db.Staging[id]; found {
		if time.Now().Sub(info.Time) >= db.EndPt.StagingDuration { // FIXME: not always so!
			return databases.StagingStatusSucceeded, nil
		}
		return databases.StagingStatusActive, nil
	} else {
		return databases.StagingStatusUnknown, nil
	}
}

func (db *Database) Endpoint() (Endpoint, error) {
	return db.Endpt, nil
}

func (db *Database) LocalUser(orcid string) (string, error) {
	return "testuser", nil
}
