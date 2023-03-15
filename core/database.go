package core

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"dts/config"
)

// pagination-related parameters
type Pagination struct {
	// number of search results to skip
	Offset int
	// maximum number of search results to include (-1 indicates no max)
	MaxNum int
}

// returns the page number and page size corresponding to the given Pagination
// parameters
func PageNumberAndSize(p Pagination) (int, int) {
	pageNumber := 1
	pageSize := 100
	if p.Offset > 0 {
		if p.MaxNum == -1 {
			pageSize = p.Offset
			pageNumber = 2
		} else {
			pageSize = p.MaxNum
			pageNumber = p.Offset/pageSize + 1
		}
	} else {
		if p.MaxNum > 0 {
			pageSize = p.MaxNum
		}
	}
	return pageNumber, pageSize
}

// file database appropriate for conducting searches
type Database struct {
	Id       string
	BaseURL  string
	Endpoint Endpoint
}

func NewDatabase(dbName string) (*Database, error) {
	dbConfig, ok := config.Databases[dbName]
	if !ok {
		return nil, fmt.Errorf("Database %s not found", dbName)
	}
	endpoint, err := NewEndpoint(dbConfig.Endpoint)
	if err != nil {
		return nil, err
	}

	return &Database{
		Id:       dbName,
		BaseURL:  dbConfig.URL,
		Endpoint: endpoint,
	}, nil
}

func (db *Database) Search(query string, pagination Pagination) (SearchResults, error) {
	// for now we assume the JDP interface for ElasticSearch queries
	// (see https://files.jgi.doe.gov/apidoc/)
	var results SearchResults

	pageNumber, pageSize := PageNumberAndSize(pagination)

	params := url.Values{}
	params.Add("q", query)
	params.Add("p", strconv.Itoa(pageNumber))
	params.Add("x", strconv.Itoa(pageSize))

	type JDPResult struct {
		Organisms []struct {
			Files []File `json:"files"`
		} `json:"organisms"`
	}

	u, err := url.ParseRequestURI(db.BaseURL)
	if err == nil {
		u.Path = "search"
		u.RawQuery = params.Encode()

		request := fmt.Sprintf("%v", u)
		resp, err := http.Get(request)
		defer resp.Body.Close()
		if err == nil {
			body, err := io.ReadAll(resp.Body)
			if err == nil {
				var jdpResults JDPResult
				results.Files = make([]File, 0)
				json.Unmarshal(body, &jdpResults)
				for _, org := range jdpResults.Organisms {
					results.Files = append(results.Files, org.Files...)
				}
			}
		}
	}

	return results, err
}
