package databases

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/google/uuid"

	"dts/config"
	"dts/core"
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

// file database appropriate for handling JDP searches and transfers
type JdpDatabase struct {
	// database identifier
	Id string
	// JDP base URL
	BaseURL string
	// API token used for authentication
	ApiToken string
	// mapping from staging UUIDs to JDP restoration request ID
	StagingIds map[uuid.UUID]int
}

func NewJdpDatabase(dbName string) (Database, error) {
	dbConfig, ok := config.Databases[dbName]
	if !ok {
		return nil, fmt.Errorf("Database %s not found", dbName)
	}

	// read our API access token from a file
	// FIXME

	return &JdpDatabase{
		Id:         dbName,
		BaseURL:    dbConfig.URL,
		StagingIds: make(map[uuid.UUID]int),
	}, nil
}

// this helper extracts files for the JDP /search GET query with given parameters
// FIXME: Currently, this returns SearchResults directly, since the core.File
// FIXME: type has fields corresponding to those in the JGI Data Portal's
// FIXME: interface. When we prune/canonicalize the core.File type, we should
// FIXME: move the old type to this file and use an array of that type for this
// FIXME: function's return value.
func (db *JdpDatabase) filesForSearch(params url.Values) (SearchResults, error) {
	var results SearchResults

	u, err := url.ParseRequestURI(db.BaseURL)
	if err == nil {
		u.Path = "search"
		u.RawQuery = params.Encode()

		request := fmt.Sprintf("%v", u)
		var resp *http.Response
		resp, err = http.Get(request)
		defer resp.Body.Close()
		if err == nil {
			var body []byte
			body, err = io.ReadAll(resp.Body)
			if err == nil {
				type JDPResult struct {
					Organisms []struct {
						Files []core.File `json:"files"`
					} `json:"organisms"`
				}
				var jdpResults JDPResult
				results.Files = make([]core.File, 0)
				err = json.Unmarshal(body, &jdpResults)
				if err == nil {
					for _, org := range jdpResults.Organisms {
						results.Files = append(results.Files, org.Files...)
					}
				}
			}
		}
	}
	return results, err
}

func (db *JdpDatabase) Search(params SearchParameters) (SearchResults, error) {
	// we assume the JDP interface for ElasticSearch queries
	// (see https://files.jgi.doe.gov/apidoc/)
	pageNumber, pageSize := pageNumberAndSize(params.Pagination.Offset, params.Pagination.MaxNum)

	p := url.Values{}
	p.Add("q", params.Query)
	p.Add("p", strconv.Itoa(pageNumber))
	p.Add("x", strconv.Itoa(pageSize))

	return db.filesForSearch(p)
}

func (db *JdpDatabase) FilesStaged(fileIds []string) (bool, error) {
	// fetch the paths for the files with the given IDs that are RESTORED
	type FileFilter struct {
		Ids      []string `json:"_id"`
		Statuses []string `json:"file_status"`
	}
	ff, err := json.Marshal(FileFilter{Ids: fileIds, Statuses: []string{"RESTORED"}})
	if err != nil {
		return false, err
	}
	p := url.Values{}
	p.Add("ff=", string(ff))
	results, err := db.filesForSearch(p)
	if err != nil {
		return false, err
	}

	// Did we get back all files we requested? If so, all files are staged.
	return len(results.Files) == len(fileIds), nil
}

func (db *JdpDatabase) StageFiles(fileIds []string) (uuid.UUID, error) {
	var xferId uuid.UUID

	// construct a POST request to restore archived files with the given IDs
	type RestoreRequest struct {
		Ids []string `json:"ids"`
	}
	data, err := json.Marshal(RestoreRequest{Ids: fileIds})
	if err != nil {
		return xferId, err
	}

	u, err := url.ParseRequestURI(db.BaseURL)
	if err == nil {
		u.Path = "request_archived_files"

		request := fmt.Sprintf("%v", u)
		var resp *http.Response
		resp, err = http.Post(request, "application/json", bytes.NewReader(data))
		defer resp.Body.Close()
		if err == nil {
			var body []byte
			body, err = io.ReadAll(resp.Body)
			if err == nil {
				type RestoreResponse struct {
					RequestId int `json:"request_id"`
				}

				var jdpResp RestoreResponse
				err = json.Unmarshal(body, &jdpResp)
				if err == nil {
					xferId = uuid.New()
					db.StagingIds[xferId] = jdpResp.RequestId
				}
			}
		}
	}

	return xferId, err
}

func (db *JdpDatabase) StagingStatus(id uuid.UUID) (StagingStatus, error) {
	if restoreId, found := db.StagingIds[id]; found {
		u, err := url.ParseRequestURI(db.BaseURL)
		if err == nil {
			u.Path = fmt.Sprintf("request_archived_files/requests/%d", restoreId)

			request := fmt.Sprintf("%v", u)
			var resp *http.Response
			resp, err = http.Get(request)
			defer resp.Body.Close()
			if err == nil {
				var body []byte
				body, err = io.ReadAll(resp.Body)
				if err == nil {
					type JDPResult struct {
						Status string `json:"status"` // "ready" or not
					}
					var jdpResult JDPResult
					err = json.Unmarshal(body, &jdpResult)
					if err == nil {
						statusForString := map[string]StagingStatus{
							"ready": Succeeded,
						}
						if status, ok := statusForString[jdpResult.Status]; ok {
							return status, nil
						} else {
							return status, fmt.Errorf("Unrecognized staging status string: %s", jdpResult.Status)
						}
					}
				}
			}
		}
		return Unknown, err
	} else {
		return Unknown, nil
	}
}
