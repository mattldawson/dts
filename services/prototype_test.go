package services

// This file defines a unit test setup for the DTS prototype service. To
// simplify the testing protocol, we implement source and destination
// test databases that support the transfer of a test payload.
import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/dtstest"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/frictionless"
)

// working directory from which the tests were invoked
var CWD string

// temporary testing directory
var TESTING_DIR string

// DTS URLs
var (
	baseUrl   = "http://localhost:8080/"
	apiPrefix = "api/v1/"
)

var (
	testUser         = "testuser"
	sourceRoot       string
	destination1Root string
	destination2Root string
)

// service instance
var service TransferService

const dtsConfig string = `
service:
  port: 8080
  max_connections: 100
  poll_interval: 100
  data_dir: TESTING_DIR/data
  delete_after: 24
  endpoint: local-endpoint
databases:
  source:
    name: Source Test Database
    organization: The Source Company
    endpoint: source-endpoint
  destination1:
    name: Destination Test Database 1
    organization: Fabulous Destinations, Inc.
    endpoint: destination-endpoint1
  destination2:
    name: Destination Test Database 2
    organization: Fabulous Destinations, Inc.
    endpoint: destination-endpoint2
endpoints:
  local-endpoint:
    name: Local endpoint
    id: 8816ec2d-4a48-4ded-b68a-5ab46a4417b6
    provider: local
    root: TESTING_DIR
  source-endpoint:
    name: Endpoint 1
    id: 26d61236-39f6-4742-a374-8ec709347f2f
    provider: local
    root: SOURCE_ROOT
  destination-endpoint1:
    name: Endpoint 2
    id: f1865b86-2c64-4b8b-99f3-5aaa945ec3d9
    provider: local
    root: DESTINATION1_ROOT
  destination-endpoint2:
    name: Endpoint 3
    id: f1865b86-2c64-4b8b-99f3-5aaa945ec3d9
    provider: local
    root: DESTINATION2_ROOT
`

//===============================
// Test Database Implementations
//===============================

type testDatabase struct {
	endpoint endpoints.Endpoint
	rootDir  string
}

func newSourceTestDatabase(orcid string) (databases.Database, error) {
	ep, err := endpoints.NewEndpoint("source-endpoint")
	return &testDatabase{
		endpoint: ep,
		rootDir:  sourceRoot,
	}, err
}

func newDestination1TestDatabase(orcid string) (databases.Database, error) {
	ep, err := endpoints.NewEndpoint("destination-endpoint1")
	return &testDatabase{
		endpoint: ep,
		rootDir:  destination1Root,
	}, err
}

func newDestination2TestDatabase(orcid string) (databases.Database, error) {
	ep, err := endpoints.NewEndpoint("destination-endpoint2")
	return &testDatabase{
		endpoint: ep,
		rootDir:  destination2Root,
	}, err
}

func (db *testDatabase) Search(params databases.SearchParameters) (databases.SearchResults, error) {
	// the test database returns the query as the file ID for IDs "1", "2", and "3".
	if params.Query == "1" || params.Query == "2" || params.Query == "3" {
		return databases.SearchResults{
			Resources: []frictionless.DataResource{
				frictionless.DataResource{
					Id:        params.Query,
					Name:      fmt.Sprintf("file%s", params.Query),
					Path:      fmt.Sprintf("file%s.txt", params.Query),
					Format:    "text",
					MediaType: "text/plain",
					Bytes:     4,
				},
			},
		}, nil
	} else {
		return databases.SearchResults{}, nil
	}
}

func (db *testDatabase) Resources(fileIds []string) ([]frictionless.DataResource, error) {
	results := make([]frictionless.DataResource, 0)
	for _, id := range fileIds {
		if id == "1" || id == "2" || id == "3" {
			results = append(results, frictionless.DataResource{
				Id:        id,
				Name:      fmt.Sprintf("file%s", id),
				Path:      fmt.Sprintf("file%s.txt", id),
				Format:    "text",
				MediaType: "text/plain",
				Bytes:     4,
			})
		} else {
			return nil, fmt.Errorf("Unrecognized file ID: %s", id)
		}
	}
	return results, nil
}

func (db *testDatabase) StageFiles(fileIds []string) (uuid.UUID, error) {
	// no need to stage files, since they're already in place; just return
	// a UUID.
	return uuid.NewUUID()
}

func (db *testDatabase) StagingStatus(stagingId uuid.UUID) (databases.StagingStatus, error) {
	// the files are already in place, so staging has always "succeeded".
	return databases.StagingStatusSucceeded, nil
}

func (db *testDatabase) Endpoint() (endpoints.Endpoint, error) {
	return db.endpoint, nil
}

func (db *testDatabase) LocalUser(orcid string) (string, error) {
	return testUser, nil
}

// performs testing setup
func setup() {
	dtstest.EnableDebugLogging()

	// jot down our CWD, create a temporary directory, and change to it
	var err error
	CWD, err = os.Getwd()
	if err != nil {
		log.Panicf("Couldn't get current working directory: %s", err)
	}
	log.Print("Creating testing directory...\n")
	TESTING_DIR, err = os.MkdirTemp(os.TempDir(), "data-transfer-service-tests-")
	if err != nil {
		log.Panicf("Couldn't create testing directory: %s", err)
	}
	os.Chdir(TESTING_DIR)

	// create source/destination directories and files
	sourceRoot = filepath.Join(TESTING_DIR, "source")
	err = os.Mkdir(sourceRoot, 0700)
	if err != nil {
		log.Panicf("Couldn't create source directory: %s", err)
	}
	destination1Root = filepath.Join(TESTING_DIR, "destination1")
	destination2Root = filepath.Join(TESTING_DIR, "destination2")
	for _, destinationDir := range []string{destination1Root, destination2Root} {
		err = os.Mkdir(destinationDir, 0700)
		if err != nil {
			log.Panicf("Couldn't create destination directory: %s", err)
		}
	}
	// create source files
	for i := 1; i <= 3; i++ {
		err = os.WriteFile(filepath.Join(sourceRoot, fmt.Sprintf("file%d.txt", i)),
			[]byte(fmt.Sprintf("This is the content of file %d.", i)), 0600)
		if err != nil {
			log.Panicf("Couldn't create source file: %s", err)
			break
		}
	}

	// read in the config file with SOURCE_ROOT and DESTINATION?_ROOT replaced
	myConfig := strings.ReplaceAll(dtsConfig, "SOURCE_ROOT", sourceRoot)
	myConfig = strings.ReplaceAll(myConfig, "DESTINATION1_ROOT", destination1Root)
	myConfig = strings.ReplaceAll(myConfig, "DESTINATION2_ROOT", destination2Root)
	myConfig = strings.ReplaceAll(myConfig, "TESTING_DIR", TESTING_DIR)
	err = config.Init([]byte(myConfig))
	if err != nil {
		log.Panicf("Couldn't initialize configuration: %s", err)
	}

	// create the DTS data directory
	os.Mkdir(config.Service.DataDirectory, 0755)

	// Start the service.
	log.Print("Starting test mapping service...\n")
	go func() {
		service, err = NewDTSPrototype()
		if err != nil {
			log.Panicf("Couldn't construct the service: %s", err.Error())
		}
		databases.RegisterDatabase("source", newSourceTestDatabase)
		databases.RegisterDatabase("destination1", newDestination1TestDatabase)
		databases.RegisterDatabase("destination2", newDestination2TestDatabase)
		err = service.Start(config.Service.Port)
		if err != nil {
			log.Panicf("Couldn't start search service: %s", err.Error())
		}
	}()

	// Give the service time to start up.
	time.Sleep(100 * time.Millisecond)

	// Change back to our original CWD.
	os.Chdir(CWD)
}

// Performs testing breakdown.
func breakdown() {

	if service != nil {
		// Gracefully shut the service down when it finishes its work.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		service.Shutdown(ctx)
	}

	if TESTING_DIR != "" {
		// Remove the testing directory and its contents.
		log.Printf("Deleting testing directory %s...\n", TESTING_DIR)
		os.RemoveAll(TESTING_DIR)
	}
}

// sends a GET query with well-formed headers
func get(resource string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, resource, http.NoBody)
	if err != nil {
		return nil, err
	}
	accessToken := os.Getenv("DTS_KBASE_DEV_TOKEN")
	b64Token := base64.StdEncoding.EncodeToString([]byte(accessToken))
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", b64Token))
	return http.DefaultClient.Do(req)
}

// sends a POST query with well-formed headers and a payload
func post(resource string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, resource, body)
	if err != nil {
		return nil, err
	}
	accessToken := os.Getenv("DTS_KBASE_DEV_TOKEN")
	b64Token := base64.StdEncoding.EncodeToString([]byte(accessToken))
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", b64Token))
	req.Header.Add("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

// sends a DELETE query with well-formed headers
func delete_(resource string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodDelete, resource, http.NoBody)
	if err != nil {
		return nil, err
	}
	accessToken := os.Getenv("DTS_KBASE_DEV_TOKEN")
	b64Token := base64.StdEncoding.EncodeToString([]byte(accessToken))
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", b64Token))
	req.Header.Add("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

// queries the service's root endpoint
func TestQueryRoot(t *testing.T) {
	assert := assert.New(t)

	resp, err := get(baseUrl)
	assert.Nil(err)

	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err)
	defer resp.Body.Close()

	var root RootResponse
	err = json.Unmarshal(respBody, &root)
	assert.Nil(err)
	assert.Equal("DTS prototype", root.Name)
	assert.Equal(version, root.Version)
}

// queries the service's databases endpoint
func TestQueryDatabases(t *testing.T) {
	assert := assert.New(t)

	resp, err := get(baseUrl + apiPrefix + "databases")
	assert.Nil(err)

	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err)
	defer resp.Body.Close()

	var dbs []dbMetadata
	err = json.Unmarshal(respBody, &dbs)
	assert.Nil(err)
	assert.Equal(3, len(dbs))
	slices.SortFunc(dbs, func(a, b dbMetadata) int { // sort alphabetically
		if a.Id < b.Id {
			return -1
		} else if a.Id == b.Id {
			return 0
		} else {
			return 1
		}
	})

	assert.Equal("destination1", dbs[0].Id)
	assert.Equal("Destination Test Database 1", dbs[0].Name)
	assert.Equal("Fabulous Destinations, Inc.", dbs[0].Organization)

	assert.Equal("destination2", dbs[1].Id)
	assert.Equal("Destination Test Database 2", dbs[1].Name)
	assert.Equal("Fabulous Destinations, Inc.", dbs[1].Organization)

	assert.Equal("source", dbs[2].Id)
	assert.Equal("Source Test Database", dbs[2].Name)
	assert.Equal("The Source Company", dbs[2].Organization)
}

// queries a specific (valid) database
func TestQueryValidDatabase(t *testing.T) {
	assert := assert.New(t)

	resp, err := get(baseUrl + apiPrefix + "databases/source")
	assert.Nil(err)

	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err)
	defer resp.Body.Close()

	var db dbMetadata
	err = json.Unmarshal(respBody, &db)
	assert.Nil(err)
	assert.Equal("source", db.Id)
	assert.Equal("Source Test Database", db.Name)
	assert.Equal("The Source Company", db.Organization)
}

// queries a database that doesn't exist
func TestQueryInvalidDatabase(t *testing.T) {
	assert := assert.New(t)

	resp, err := get(baseUrl + apiPrefix + "databases/nonexistentdb")
	assert.Nil(err)
	assert.Equal(http.StatusNotFound, resp.StatusCode)
}

// searches a specific database for files matching a simple query
func TestSearchDatabase(t *testing.T) {
	assert := assert.New(t)

	// our source test database returns all requested source files
	resp, err := get(baseUrl + apiPrefix + "files?database=source&query=1")
	assert.Nil(err)

	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err)
	assert.Equal(http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()

	var results ElasticSearchResponse
	err = json.Unmarshal(respBody, &results)
	assert.Nil(err)
	assert.Equal("source", results.Database)
	assert.Equal("1", results.Query)
	assert.Equal(1, len(results.Resources))
	assert.Equal("file1", results.Resources[0].Name)
}

// creates a transfer from source -> destination1
func TestCreateTransfer(t *testing.T) {
	assert := assert.New(t)

	// request a transfer of file1.txt, file2.txt, and file3.txt
	payload, err := json.Marshal(TransferRequest{
		Source:      "source",
		FileIds:     []string{"1", "2", "3"},
		Destination: "destination1",
	})
	resp, err := post(baseUrl+apiPrefix+"transfers", bytes.NewReader(payload))
	assert.Nil(err)
	assert.Equal(http.StatusCreated, resp.StatusCode)
	defer resp.Body.Close()
	var body []byte
	body, err = io.ReadAll(resp.Body)
	assert.Nil(err)
	var xferResp TransferResponse
	err = json.Unmarshal(body, &xferResp)
	assert.Nil(err)
	xferId := xferResp.Id

	// get the transfer status
	queryTransfer := func() (TransferStatusResponse, error) {
		resp, err := get(baseUrl + apiPrefix + fmt.Sprintf("transfers/%s", xferId.String()))
		assert.Nil(err)
		assert.Equal(http.StatusOK, resp.StatusCode)
		var body []byte
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		assert.Nil(err)
		var statusResp TransferStatusResponse
		err = json.Unmarshal(body, &statusResp)
		return statusResp, err
	}

	status, err := queryTransfer()
	assert.Nil(err)
	assert.True(status.Status != "failed")

	// wait a bit for the task to finish (shouldn't take long)
	time.Sleep(600 * time.Millisecond)

	// query the transfer again
	status, err = queryTransfer()
	assert.Nil(err)
	assert.True(status.Status == "succeeded")

	// check for the files in the payload
	// FIXME: the files are written to the destination endpoint's root in a
	// FIXME: user-specific and task-specific folder. We need to formalize this.
	username := testUser
	for _, file := range []string{"file1.txt", "file2.txt", "file3.txt", "manifest.json"} {
		_, err := os.Stat(filepath.Join(destination1Root, username, xferId.String(), file))
		assert.Nil(err)
	}
}

// creates a transfer from source -> destination2 and then cancels it
func TestCreateAndCancelTransfer(t *testing.T) {
	assert := assert.New(t)

	// request a transfer of file1.txt, file2.txt, and file3.txt
	payload, err := json.Marshal(TransferRequest{
		Source:      "source",
		FileIds:     []string{"1", "2", "3"},
		Destination: "destination2",
	})
	resp, err := post(baseUrl+apiPrefix+"transfers", bytes.NewReader(payload))
	assert.Nil(err)
	assert.Equal(http.StatusCreated, resp.StatusCode)
	defer resp.Body.Close()
	var body []byte
	body, err = io.ReadAll(resp.Body)
	assert.Nil(err)
	var xferResp TransferResponse
	err = json.Unmarshal(body, &xferResp)
	assert.Nil(err)
	xferId := xferResp.Id

	// these are functions for querying and canceling the transfer
	queryTransfer := func() (TransferStatusResponse, error) {
		var statusResp TransferStatusResponse
		resp, err := get(baseUrl + apiPrefix + fmt.Sprintf("transfers/%s", xferId.String()))
		if err != nil {
			return statusResp, err
		}
		assert.Equal(http.StatusOK, resp.StatusCode)
		var body []byte
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return statusResp, err
		}
		err = json.Unmarshal(body, &statusResp)
		return statusResp, err
	}
	cancelTransfer := func() error {
		resp, err := delete_(baseUrl + apiPrefix + fmt.Sprintf("transfers/%s", xferId.String()))
		if err != nil {
			return err
		}
		assert.Equal(http.StatusAccepted, resp.StatusCode)
		return err
	}

	// get the transfer status
	status, err := queryTransfer()
	assert.Nil(err)
	assert.True(status.Status != "failed")

	// cancel the transfer
	err = cancelTransfer()

	// wait for the transfer to finish or be canceled
	status, err = queryTransfer()
	assert.Nil(err)
	for {
		if status.Status == "succeeded" || status.Status == "failed" {
			break
		}
		time.Sleep(600 * time.Millisecond)
		status, err = queryTransfer()
	}
}

// attempts to fetch the status of a nonexistent transfer
func TestFetchInvalidTransferStatus(t *testing.T) {
	assert := assert.New(t)

	// try an ill-formed transfer
	resp, err := get(baseUrl + apiPrefix + "transfers/xyzzy")
	assert.Nil(err)
	assert.Equal(http.StatusBadRequest, resp.StatusCode)

	// I bet this one doesn't exist!!
	resp, err = get(baseUrl + apiPrefix + "transfers/3f0f9563-e1f8-4b9c-9308-36988e25df0b")
	assert.Nil(err)
	assert.Equal(http.StatusNotFound, resp.StatusCode)
}

// runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	var status int
	setup()
	if TESTING_DIR != "" {
		status = m.Run()
	}
	breakdown()
	os.Exit(status)
}
