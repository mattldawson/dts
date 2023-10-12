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
	"math/rand"
	"net/http"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/core"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints/globus"
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

// service instance
var service TransferService

const dtsConfig string = `
service:
  port: 8080
  max_connections: 100
  poll_interval: 100
databases:
  source:
    name: Source Test Database
    organization: The Source Company
    endpoint: source-endpoint
  destination:
    name: Destination Test Database
    organization: Fabulous Destinations, Inc.
    endpoint: destination-endpoint
endpoints:
  source-endpoint:
    name: Globus Tutorial Endpoint 1
    id: ddb59aef-6d04-11e5-ba46-22000b92c6ec
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
  destination-endpoint:
    name: Globus Tutorial Endpoint 2
    id: ddb59af0-6d04-11e5-ba46-22000b92c6ec
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
`

//===============================
// Test Database Implementations
//===============================
// Our source and destination test databases are thin wrappers around a
// collection of files on a couple of Globus endpoints. These endpoints
// are used to illustrate a simple file transfer here:
// https://globus-sdk-python.readthedocs.io/en/stable/examples/minimal_transfer_script/index.html#example-minimal-transfer

type testDatabase struct {
	endpoint core.Endpoint
	fileDir  string // directory housing files
}

func newSourceTestDatabase(orcid string) (core.Database, error) {
	ep, err := globus.NewEndpoint("source-endpoint")
	return &testDatabase{
		endpoint: ep,
		fileDir:  "share/godata",
	}, err
}

// This function generates a unique name for a directory on the destination
// endpoint to receive files
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func destDirName(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func newDestinationTestDatabase(orcid string) (core.Database, error) {
	ep, err := globus.NewEndpoint("destination-endpoint")
	return &testDatabase{
		endpoint: ep,
		fileDir:  destDirName(16),
	}, err
}

func (db *testDatabase) Search(params core.SearchParameters) (core.SearchResults, error) {
	// the test database returns the query as the file ID for IDs "1", "2", and "3".
	if params.Query == "1" || params.Query == "2" || params.Query == "3" {
		return core.SearchResults{
			Resources: []core.DataResource{
				core.DataResource{
					Id:        params.Query,
					Name:      fmt.Sprintf("file%s", params.Query),
					Path:      fmt.Sprintf("%s/file%s.txt", db.fileDir, params.Query),
					Format:    "text",
					MediaType: "text/plain",
					Bytes:     4,
				},
			},
		}, nil
	} else {
		return core.SearchResults{}, nil
	}
}

func (db *testDatabase) Resources(fileIds []string) ([]core.DataResource, error) {
	results := make([]core.DataResource, 0)
	for _, id := range fileIds {
		if id == "1" || id == "2" || id == "3" {
			results = append(results, core.DataResource{
				Id:        id,
				Name:      fmt.Sprintf("file%s", id),
				Path:      fmt.Sprintf("%s/file%s.txt", db.fileDir, id),
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

func (db *testDatabase) StagingStatus(stagingId uuid.UUID) (core.StagingStatus, error) {
	// the files are already in place, so staging has always "succeeded".
	return core.StagingStatusSucceeded, nil
}

func (db *testDatabase) Endpoint() core.Endpoint {
	return db.endpoint
}

// performs testing setup
func setup() {
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

	// initialize the config environment
	err = config.Init([]byte(dtsConfig))
	if err != nil {
		log.Panic(fmt.Sprintf("Couldn't initialize test configuration: %s", err.Error()))
	}

	// Start the service.
	log.Print("Starting test mapping service...\n")
	go func() {
		service, err = NewDTSPrototype()
		if err != nil {
			log.Panicf("Couldn't construct the service: %s", err.Error())
		}
		databases.RegisterDatabase("source", newSourceTestDatabase)
		databases.RegisterDatabase("destination", newDestinationTestDatabase)
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
	if err == nil {
		accessToken := os.Getenv("DTS_KBASE_DEV_TOKEN")
		b64Token := base64.StdEncoding.EncodeToString([]byte(accessToken))
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", b64Token))
		return http.DefaultClient.Do(req)
	} else {
		return nil, err
	}
}

// sends a POST query with well-formed headers and a payload
func post(resource string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, resource, body)
	if err == nil {
		accessToken := os.Getenv("DTS_KBASE_DEV_TOKEN")
		b64Token := base64.StdEncoding.EncodeToString([]byte(accessToken))
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", b64Token))
		req.Header.Add("Content-Type", "application/json")
		return http.DefaultClient.Do(req)
	} else {
		return nil, err
	}
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
	assert.Equal(core.Version, root.Version)
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
	assert.Equal(2, len(dbs))
	slices.SortFunc(dbs, func(a, b dbMetadata) int { // sort alphabetically
		if a.Id < b.Id {
			return -1
		} else if a.Id == b.Id {
			return 0
		} else {
			return 1
		}
	})

	assert.Equal("destination", dbs[0].Id)
	assert.Equal("Destination Test Database", dbs[0].Name)
	assert.Equal("Fabulous Destinations, Inc.", dbs[0].Organization)

	assert.Equal("source", dbs[1].Id)
	assert.Equal("Source Test Database", dbs[1].Name)
	assert.Equal("The Source Company", dbs[1].Organization)
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
	assert.Equal(404, resp.StatusCode)
}

// searches a specific database for files matching a simple query
func TestSearchDatabase(t *testing.T) {
	assert := assert.New(t)

	// our source test database returns all requested source files
	resp, err := get(baseUrl + apiPrefix + "files?database=source&query=1")
	assert.Nil(err)

	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err)
	assert.Equal(200, resp.StatusCode)
	defer resp.Body.Close()

	var results ElasticSearchResponse
	err = json.Unmarshal(respBody, &results)
	assert.Nil(err)
	assert.Equal("source", results.Database)
	assert.Equal("1", results.Query)
	assert.Equal(1, len(results.Resources))
	assert.Equal("file1", results.Resources[0].Name)
}

// creates a transfer for a single file
func TestCreateTransfer(t *testing.T) {
	assert := assert.New(t)

	// request a transfer of file1.txt, file2.txt, and file3.txt
	payload, err := json.Marshal(TransferRequest{
		Source:      "source",
		FileIds:     []string{"1", "2", "3"},
		Destination: "destination",
	})
	resp, err := post(baseUrl+apiPrefix+"transfers", bytes.NewReader(payload))
	assert.Nil(err)
	assert.Equal(200, resp.StatusCode)
	if err == nil {
		defer resp.Body.Close()
		var body []byte
		body, err = io.ReadAll(resp.Body)
		assert.Nil(err)
		var xferResp TransferResponse
		err = json.Unmarshal(body, &xferResp)
		xferId := xferResp.Id

		// get the transfer status
		resp, err := get(baseUrl + apiPrefix + fmt.Sprintf("transfers/%s", xferId.String()))
		assert.Nil(err)
		assert.Equal(200, resp.StatusCode)
	}
}

// attempts to fetch the status of a nonexistent transfer
func TestFetchInvalidTransferStatus(t *testing.T) {
	assert := assert.New(t)

	// try an ill-formed transfer
	resp, err := get(baseUrl + apiPrefix + "transfers/xyzzy")
	assert.Nil(err)
	assert.Equal(400, resp.StatusCode)

	// I bet this one doesn't exist!!
	resp, err = get(baseUrl + apiPrefix + "transfers/3f0f9563-e1f8-4b9c-9308-36988e25df0b")
	assert.Nil(err)
	assert.Equal(404, resp.StatusCode)
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
