package globus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/google/uuid"

	"dts/config"
	"dts/core"
)

// This file implements a Globus endpoint. It uses the Globus Transfer API
// described at https://docs.globus.org/api/transfer/.

const (
	globusTransferBaseURL = "https://transfer.api.globusonline.org/v0.10"
)

// this type captures results from Globus Transfer API responses, including
// any errors encountered (https://docs.globus.org/api/transfer/overview/#errors)
type globusResult struct {
	// string indicating the Globus error condition (e.g. "EndpointNotFound")
	Code string `json:"code"`
	// error message
	Message string `json:"message"`
}

// this type satisfies the core.Endpoint interface for Globus endpoints
type Endpoint struct {
	// descriptive endpoint name (obtained from config)
	Name string
	// endpoint UUID (obtained from config)
	Id uuid.UUID
	// HTTPS header containing basic authentication info
	Header http.Header
}

func NewEndpoint(endpointName string) (core.Endpoint, error) {
	epConfig, found := config.Globus.Endpoints[endpointName]
	if !found {
		return nil, fmt.Errorf("'%s' is not a Globus endpoint", endpointName)
	}

	ep := &GlobusEndpoint{
		Name:   epConfig.Name,
		Id:     epConfig.Id,
		Header: make(http.Header),
	}

	// authenticate to obtain a Globus Transfer API access token
	err := ep.authenticate(config.Globus.Auth.ClientId,
		config.Globus.Auth.ClientSecret)

	if err == nil {
		// auto-activate the endpoint so we can use it
		err = ep.autoActivate()
	}

	return ep, err
}

// authenticates with Globus using a client ID and secret to obtain an access
// token (https://docs.globus.org/api/auth/reference/#client_credentials_grant)
func (ep *Endpoint) authenticate(clientId uuid.UUID, clientSecret string) error {
	authUrl := "https://auth.globus.org/v2/oauth2/token"
	data := url.Values{}
	data.Set("scope", "urn:globus:auth:scope:transfer.api.globus.org:all")
	data.Set("grant_type", "client_credentials")
	req, err := http.NewRequest(http.MethodPost, authUrl, strings.NewReader(data.Encode()))
	if err == nil {
		// set up request headers
		req.SetBasicAuth(config.Globus.Auth.ClientId.String(),
			config.Globus.Auth.ClientSecret)
		req.Header.Add("Content-Type", "application-x-www-form-urlencoded")

		// send the request
		client := &http.Client{}
		var resp *http.Response
		resp, err = client.Do(req)
		if err == nil {
			// read and unmarshal the response
			buffer := make([]byte, resp.ContentLength)
			_, err = resp.Body.Read(buffer)
			if err == nil {
				type AuthResponse struct {
					AccessToken    string `json:"access_token"`
					Scope          string `json:"scope"`
					ResourceServer string `json:"resource_server"`
					ExpiresIn      int    `json:"expires_in"`
					TokenType      string `json:"token_type"`
				}

				var authResponse AuthResponse
				err = json.Unmarshal(buffer, &authResponse)
				if err == nil {
					// stash the access token in our HTTPS header
					ep.Header.Add("Authorization", fmt.Sprintf("Bearer %s", authResponse.AccessToken))
				}
			}
		}
	}
	return err
}

// auto-activates a Globus endpoint so we can access Transfer API resources
// (https://docs.globus.org/api/transfer/endpoint_activation/#autoactivate_endpoint)
func (ep *Endpoint) autoActivate() error {
	activateUrl := fmt.Sprintf("%s/endpoint/%s/autoactivate",
		globusTransferBaseURL, ep.Id)
	req, err := http.NewRequest(http.MethodPost, activateUrl, nil)
	if err == nil {
		req.Header = ep.Header

		// send the request
		client := &http.Client{}
		var resp *http.Response
		resp, err = client.Do(req)

		// inspect the result
		if err == nil && resp.StatusCode != 200 {
			// read and unmarshal the response
			buffer := make([]byte, resp.ContentLength)
			_, err = resp.Body.Read(buffer)
			var result globusResult
			json.Unmarshal(buffer, &result)
			err = fmt.Errorf("Error in Globus endpoint auto-activation (%s): %s",
				result.Code, result.Message)
		}
	}
	return err
}

func (ep *Endpoint) FilesStaged(filePaths []string) (bool, error) {
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

		u, err := url.ParseRequestURI(globusTransferBaseURL)
		if err == nil {
			u.Path = fmt.Sprintf("operation/endpoint/%s", ep.Id)
			u.RawQuery = p.Encode()

			request := fmt.Sprintf("%v", u)
			var resp *http.Response
			resp, err = http.Get(request)
			defer resp.Body.Close()
			if err == nil {
				var body []byte
				body, err = io.ReadAll(resp.Body)
				if err == nil {
					// https://docs.globus.org/api/transfer/file_operations/#dir_listing_response
					type DirListingResponse struct {
						Data []struct {
							Name string `json:"name"`
						} `json:"DATA"`
					}
					var response DirListingResponse
					err = json.Unmarshal(body, &response)
					if err == nil {
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
		}
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func (ep *Endpoint) Transfers() ([]uuid.UUID, error) {
	// https://docs.globus.org/api/transfer/task/#get_task_list
	p := url.Values{}
	p.Add("fields", "task_id")
	p.Add("filter", "status:ACTIVE,INACTIVE/label:DTS")
	p.Add("limit", "1000")
	p.Add("orderby", "name ASC")

	u, err := url.ParseRequestURI(globusTransferBaseURL)
	if err == nil {
		u.Path = "/task_list"
		u.RawQuery = p.Encode()

		request := fmt.Sprintf("%v", u)
		var resp *http.Response
		resp, err = http.Get(request)
		defer resp.Body.Close()
		if err == nil {
			var body []byte
			body, err = io.ReadAll(resp.Body)
			if err == nil {
				type TaskListResponse struct {
					Length int `json:"length"`
					Limit  int `json:"limіt"`
					Data   []struct {
						TaskId uuid.UUID `json:"task_id"`
					} `json:"DATA"`
				}
				var response TaskListResponse
				err = json.Unmarshal(body, &response)
				if err == nil {
					taskIds := make([]uuid.UUID, len(response.Data))
					for i, data := range response.Data {
						taskIds[i] = data.TaskId
					}
					return taskIds, nil
				}
			}
		}
	}
	return nil, err
}

func (ep *Endpoint) Transfer(dst core.Endpoint, files []FileTransfer) (uuid.UUID, error) {
	gDst := dst.(*globus.Endpoint)
	var xferId uuid.UUID
	u, err := url.ParseRequestURI(globusTransferBaseURL)
	if err == nil {
		// first, get a submission ID
		// https://docs.globus.org/api/transfer/task_submit/#get_submission_id
		u.Path = "/submission_id"
		request := fmt.Sprintf("%v", u)
		var resp *http.Response
		resp, err = http.Get(request)
		defer resp.Body.Close()
		if err == nil {
			var body []byte
			body, err = io.ReadAll(resp.Body)
			if err == nil {
				type SubmissionIdResponse struct {
					Value uuid.UUID `json:"value"`
				}
				var response SubmissionIdResponse
				err = json.Unmarshal(body, &response)
				if err == nil {
					xferId = response.Value
				}
			}
		}

		if err == nil {
			// now, submit the transfer task itself
			// https://docs.globus.org/api/transfer/task_submit/#submit_transfer_task
			// https://docs.globus.org/api/transfer/task_submit/#transfer_item_fields
			type TransferItem struct {
				DataType         string `json:"DATA_TYPE"` // "transfer_item"
				SourcePath       string `json:"source_path"`
				DestinationPath  string `json:"destination_path"`
				Recursive        bool   `json:"recursive"`         // false
				ExternalChecksum string `json:"external_checksum"` // md5 checksum
			}
			type SubmissionRequest struct {
				DataType            string         `json":DATA_TYPE"` // "transfer"
				Id                  string         `json:"submission_id"`
				Label               string         `json:"label"` // "DTS"
				Data                []TransferItem `json:"DATA"`
				DestinationEndpoint string         `json:"destination_endpoint"`
				SourceEndpoint      string         `json:"source_endpoint"`
				SyncLevel           int            `json:"sync_level"`
				VerifyChecksum      bool           `json:"verify_checksum"`
				FailOnQuotaErrors   bool           `json:"fail_on_quota_errors"`
			}
			xferItems := make([]TransferItem, len(files))
			for i, file := range files {
				xferItems[i] = TransferItem{
					DataType:         "transfer_item",
					SourcePath:       file.SourcePath,
					DestinationPath:  file.DestinationPath,
					Recursive:        false,
					ExternalChecksum: file.MD5Checksum,
				}
			}
			var data []byte
			data, err = json.Marshal(SubmissionRequest{
				DataType:            "transfer",
				Id:                  xferId.String(),
				Label:               "DTS",
				Data:                xferItems,
				DestinationEndpoint: gDst.Id.String(),
				SourceEndpoint:      ep.Id.String(),
				SyncLevel:           3, // transfer only if checksums don't match
				VerifyChecksum:      true,
				FailOnQuotaErrors:   true,
			})
			if err == nil {
				u.Path = "/transfer"
				request := fmt.Sprintf("%v", u)
				var resp *http.Response
				resp, err = http.Post(request, "application/json", bytes.NewReader(data))
				defer resp.Body.Close()
				if err == nil {
					var body []byte
					body, err = io.ReadAll(resp.Body)
					if err == nil {
						type SubmissionResponse struct {
							TaskId uuid.UUID `json:"task_id"`
						}

						var gResp SubmissionResponse
						err = json.Unmarshal(body, &gResp)
						if err == nil {
							xferId = gResp.TaskId
						}
					}
				}
			}
		}
	}

	return xferId, err
}

func (ep *Endpoint) Status(id uuid.UUID) (core.TransferStatus, error) {
	u, err := url.ParseRequestURI(globusTransferBaseURL)
	if err == nil {
		u.Path = fmt.Sprintf("/task/%s", id.String())

		request := fmt.Sprintf("%v", u)
		var resp *http.Response
		resp, err = http.Get(request)
		defer resp.Body.Close()
		if err == nil {
			var body []byte
			body, err = io.ReadAll(resp.Body)
			if err == nil {
				type TaskResponse struct {
					Files            int    `json:"files"`
					FilesTransferred int    `json:"files_transferred"`
					IsPaused         bool   `json:"is_paused"`
					Status           string `json:"status"`
				}
				var response TaskResponse
				err = json.Unmarshal(body, &response)
				if err == nil {
					codes := map[string]TransferStatusCode{
						"Active":    Active,
						"Inactive":  Inactive,
						"Succeeded": Succeeded,
						"Failed":    Failed,
					}
					return TransferStatus{
						StatusCode:          codes[response.Status],
						NumFiles:            response.Files,
						NumFilesTransferred: response.FilesTransferred,
						Paused:              response.IsPaused,
					}, nil
				}
			}
		}
	}
	return TransferStatus{}, err
}

func (ep *Endpoint) Cancel(id uuid.UUID) error {
	// https://docs.globus.org/api/transfer/task/#cancel_task_by_id
	u, err := url.ParseRequestURI(globusTransferBaseURL)
	if err == nil {
		u.Path = fmt.Sprintf("/task/%s/cancel", id.String())

		request := fmt.Sprintf("%v", u)
		var resp *http.Response
		resp, err = http.Get(request)
		defer resp.Body.Close()
		if err == nil {
			// FIXME
			//var body []byte
			//body, err = io.ReadAll(resp.Body)
		}
	}
	return err
}
