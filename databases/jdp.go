package databases

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/google/uuid"

	"dts/config"
	"dts/core"
	"dts/endpoints"
)

// returns the page number and page size corresponding to the given Pagination
// parameters
func pageNumberAndSize(offset, maxNum int) (int, int) {
	pageNumber := 1
	pageSize := 100
	if offset > 0 {
		if maxNum == -1 {
			pageSize = offset
			pageNumber = 2
		} else {
			pageSize = maxNum
			pageNumber = offset/pageSize + 1
		}
	} else {
		if maxNum > 0 {
			pageSize = maxNum
		}
	}
	return pageNumber, pageSize
}

// file database appropriate for conducting searches
type JdpDatabase struct {
	Id       string
	BaseURL  string
	Endpoint endpoints.Endpoint
	ApiToken string
}

func NewJdpDatabase(dbName string) (Database, error) {
	dbConfig, ok := config.Databases[dbName]
	if !ok {
		return nil, fmt.Errorf("Database %s not found", dbName)
	}
	endpoint, err := endpoints.NewEndpoint(dbConfig.Endpoint)
	if err != nil {
		return nil, err
	}

	// read our API access token from a file
	// FIXME

	return &JdpDatabase{
		Id:       dbName,
		BaseURL:  dbConfig.URL,
		Endpoint: endpoint,
	}, nil
}

func (db *JdpDatabase) Search(params SearchParameters) (SearchResults, error) {
	// we assume the JDP interface for ElasticSearch queries
	// (see https://files.jgi.doe.gov/apidoc/)
	var results SearchResults

	pageNumber, pageSize := pageNumberAndSize(params.Pagination.Offset, params.Pagination.MaxNum)

	p := url.Values{}
	p.Add("q", params.Query)
	p.Add("p", strconv.Itoa(pageNumber))
	p.Add("x", strconv.Itoa(pageSize))

	type JDPResult struct {
		Organisms []struct {
			Files []core.File `json:"files"`
		} `json:"organisms"`
	}

	u, err := url.ParseRequestURI(db.BaseURL)
	if err == nil {
		u.Path = "search"
		u.RawQuery = p.Encode()

		request := fmt.Sprintf("%v", u)
		resp, err := http.Get(request)
		defer resp.Body.Close()
		if err == nil {
			body, err := io.ReadAll(resp.Body)
			if err == nil {
				var jdpResults JDPResult
				results.Files = make([]core.File, 0)
				json.Unmarshal(body, &jdpResults)
				for _, org := range jdpResults.Organisms {
					results.Files = append(results.Files, org.Files...)
				}
			}
		}
	}

	return results, err
}

func (db *JdpDatabase) FilesStaged(fileIds []string) bool {
	return false
}

func (db *JdpDatabase) StageFiles(fileIds []string) (Transfer, error) {
	return Transfer{Id: uuid.New()}, nil
}
