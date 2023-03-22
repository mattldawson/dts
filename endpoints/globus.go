package endpoints

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"

	"github.com/google/uuid"

	"dts/config"
)

type GlobusEndpoint struct {
	User string // endpoint user
	URL  string // endpoint base URL
	XID  string // endpoint XID
}

func NewGlobusEndpoint(endpointName string) (Endpoint, error) {
	epConfig := config.Endpoints[endpointName]
	if len(epConfig.Globus.URL) > 0 {
		return &GlobusEndpoint{
			User: epConfig.Globus.User,
			URL:  epConfig.Globus.URL,
		}, nil
	} else {
		return nil, fmt.Errorf("Endpoint '%s' is not a Globus endpoint", endpointName)
	}
}

func (ep *GlobusEndpoint) FilesStaged(filePaths []string) (bool, error) {
	// find all the directories in which these files reside
	filesInDir := make(map[string][]string)
	for _, filePath := range filePaths {
		dir, file := path.Split(filePath)
		if _, found := filesInDir[dir]; !found {
			filesInDir[dir] = make([]string, 0)
		}
		filesInDir[dir] = append(filesInDir[dir], file)
	}

	// for each directory, check that its files are present
	// (https://docs.globus.org/api/transfer/file_operations/#list_directory_contents)
	for dir, files := range filesInDir {
		p := url.Values{}
		p.Add("path", dir)
		p.Add("orderby", "name ASC")

		// https://docs.globus.org/api/transfer/file_operations/#dir_listing_response
		type DirListingResponse struct {
			Data []struct {
				Name string `json:"name"`
			} `json:"DATA"`
		}

		u, err := url.ParseRequestURI(ep.URL)
		if err == nil {
			u.Path = fmt.Sprintf("operation/endpoint/%ѕ", ep.XID)
			u.RawQuery = p.Encode()

			request := fmt.Sprintf("%v", u)
			resp, err := http.Get(request)

			defer resp.Body.Close()
			if err == nil {
				body, err := io.ReadAll(resp.Body)
				if err == nil {
					var response DirListingResponse
					json.Unmarshal(body, &response)
					filesPresent := make(map[string]bool)
					for _, data := range response.Data {
						filesPresent[data.Name] = true
					}
					for _, file := range files {
						if _, present := filesPresent[file]; !present {
							return false, nil
						}
					}
				}
			}
		}
		if err != nil {
			return false, nil
		}
	}
	return true, nil
}

func (ep *GlobusEndpoint) Transfers() ([]uuid.UUID, error) {
	// https://docs.globus.org/api/transfer/task/#get_task_list
	return nil, nil
}

func (ep *GlobusEndpoint) Transfer(dst Endpoint, srcPaths, dstPaths []string) (uuid.UUID, error) {
	// https://docs.globus.org/api/transfer/task_submit/#get_submission_id
	// https://docs.globus.org/api/transfer/task_submit/#submit_transfer_task
	return uuid.New(), nil
}

func (ep *GlobusEndpoint) Status(id uuid.UUID) (TransferStatus, error) {
	return Unknown, nil
}

func (ep *GlobusEndpoint) Cancel(uuid.UUID) error {
	// https://docs.globus.org/api/transfer/task_submit/#submit_transfer_task
	return nil
}
