package services

// unit test setup for the DTS prototype service
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
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"dts/config"
	"dts/core"
)

// working directory from which the tests were invoked
var CWD string

// temporary testing directory
var TESTING_DIR string

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
  jdp:
    name: JGI Data Portal
    organization: Joint Genome Institute
    url: https://files.jgi.doe.gov
    endpoint: globus-jdp
    auth:
      client_id: ${JGI_CLIENT_ID}
      client_secret: ${JGI_CLIENT_SECRET}
  kbase:
    name: KBase
    organization: KBase
    url: https://kbase.us
    endpoint: globus-kbase
endpoints:
  globus-jdp:
    name: Globus NERSC DTN
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
  globus-kbase:
    name: Globus NERSC DTN
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
`

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
	slices.SortFunc(dbs, func(a, b dbMetadata) int {
		if a.Id < b.Id {
			return -1
		} else if a.Id == b.Id {
			return 0
		} else {
			return 1
		}
	})
	assert.Equal("jdp", dbs[0].Id)
	assert.Equal("JGI Data Portal", dbs[0].Name)
	assert.Equal("Joint Genome Institute", dbs[0].Organization)
	assert.Equal("kbase", dbs[1].Id)
	assert.Equal("KBase", dbs[1].Name)
	assert.Equal("KBase", dbs[1].Organization)
}

// queries a specific (valid) database
func TestQueryValidDatabase(t *testing.T) {
	assert := assert.New(t)

	resp, err := get(baseUrl + apiPrefix + "databases/jdp")
	assert.Nil(err)

	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err)
	defer resp.Body.Close()

	var db dbMetadata
	err = json.Unmarshal(respBody, &db)
	assert.Nil(err)
	assert.Equal("jdp", db.Id)
	assert.Equal("JGI Data Portal", db.Name)
	assert.Equal("Joint Genome Institute", db.Organization)
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

	resp, err := get(baseUrl + apiPrefix + "files?database=jdp&query=prochlorococcus")
	assert.Nil(err)

	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err)
	defer resp.Body.Close()

	var results ElasticSearchResponse
	err = json.Unmarshal(respBody, &results)
	assert.Nil(err)
	assert.Equal("jdp", results.Database)
	assert.Equal("prochlorococcus", results.Query)
	assert.NotNil(results.Resources)
}

// creates a transfer for a single file
func TestCreateTransfer(t *testing.T) {
	assert := assert.New(t)

	// The JDP database currently relies on JAMO to locate files with given
	// IDs, so we can only run this test when the DTS is running within LBL's
	// virtual private network.
	if _, onVPN := os.LookupEnv("DTS_ON_LBL_VPN"); onVPN { // any value will work
		// request a transfer of Ga0451497_cog.gff
		payload, err := json.Marshal(TransferRequest{
			Source:      "jdp",
			FileIds:     []string{"JDP:5fa4fb4547675a20c852c5f8"},
			Destination: "kbase",
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
	} else {
		log.Printf("Skipping (JAMO-backed) JDP transfer tests")
		log.Printf("To enable these tests, set the DTS_ON_LBL_VPN environment variable")
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
