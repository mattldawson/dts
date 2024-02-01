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

package globus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/frictionless"
)

// This file implements a Globus endpoint. It uses the Globus Transfer API
// described at https://docs.globus.org/api/transfer/.

const (
	globusTransferBaseURL    = "https://transfer.api.globusonline.org"
	globusTransferApiVersion = "v0.10"
)

// this type captures results from Globus Transfer API responses, including
// any errors encountered (https://docs.globus.org/api/transfer/overview/#errors)
type globusResult struct {
	// string indicating the Globus error condition (e.g. "EndpointNotFound")
	Code string `json:"code"`
	// error message
	Message string `json:"message"`
}

// this type satisfies the endpoints.Endpoint interface for Globus endpoints
type Endpoint struct {
	// descriptive endpoint name (obtained from config)
	Name string
	// endpoint UUID (obtained from config)
	Id uuid.UUID
	// root directory of endpoint (host_root)
	root string
	// HTTP client that caches queries
	Client http.Client
	// OAuth2 access token
	AccessToken string
}

// creates a new Globus endpoint using the information supplied in the
// DTS configuration file under the given endpoint name
func NewEndpoint(endpointName string) (endpoints.Endpoint, error) {
	epConfig, found := config.Endpoints[endpointName]
	if !found {
		return nil, fmt.Errorf("'%s' is not an endpoint", endpointName)
	}
	if epConfig.Provider != "globus" {
		return nil, fmt.Errorf("'%s' is not a Globus endpoint", endpointName)
	}
	if epConfig.Root != "" {
		return nil, fmt.Errorf("As a Globus endpoint, '%s' cannot have its root directory specified", endpointName)
	}

	ep := &Endpoint{
		Name: epConfig.Name,
		Id:   epConfig.Id,
	}

	// if needed, authenticate to obtain a Globus Transfer API access token
	var zeroId uuid.UUID
	if epConfig.Auth.ClientId != zeroId { // nonzero value
		err := ep.authenticate(epConfig.Auth.ClientId, epConfig.Auth.ClientSecret)
		if err != nil {
			return ep, err
		}
	}

	// fetch the endpoint's root path
	resource := fmt.Sprintf("endpoint/%s", ep.Id.String())
	resp, err := ep.get(resource, url.Values{})
	if err != nil {
		return ep, err
	}
	if resp.StatusCode != 200 {
		return ep, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ep, err
	}
	type EndpointDocument struct {
		HostRoot string `json:"host_root"`
	}
	var endpointResp EndpointDocument
	err = json.Unmarshal(body, &endpointResp)
	if err != nil {
		return ep, err
	}
	ep.root = endpointResp.HostRoot
	return ep, nil
}

// authenticates with Globus using a client ID and secret to obtain an access
// token (https://docs.globus.org/api/auth/reference/#client_credentials_grant)
func (ep *Endpoint) authenticate(clientId uuid.UUID, clientSecret string) error {
	authUrl := "https://auth.globus.org/v2/oauth2/token"
	data := url.Values{}
	data.Set("scope", "urn:globus:auth:scope:transfer.api.globus.org:all")
	data.Set("grant_type", "client_credentials")
	req, err := http.NewRequest(http.MethodPost, authUrl, strings.NewReader(data.Encode()))
	if err != nil {
		return err
	}
	req.SetBasicAuth(clientId.String(), clientSecret)
	req.Header.Add("Content-Type", "application-x-www-form-urlencoded")

	// send the request
	resp, err := ep.Client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("Couldn't authenticate via Globus Auth API (%d)", resp.StatusCode)
	}

	// read and unmarshal the response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	type AuthResponse struct {
		AccessToken    string `json:"access_token"`
		Scope          string `json:"scope"`
		ResourceServer string `json:"resource_server"`
		ExpiresIn      int    `json:"expires_in"`
		TokenType      string `json:"token_type"`
	}

	var authResponse AuthResponse
	err = json.Unmarshal(body, &authResponse)
	if err != nil {
		return err
	}

	// stash the access token
	ep.AccessToken = authResponse.AccessToken

	return nil
}

// constructs a new request to the auth server with the correct headers, etc
// * method can be http.MethodGet, http.MethodPut, http.MethodPost, etc
// * resource is the name of the desired endpoint/resource
// * body can be http.NoBody
func (ep *Endpoint) newRequest(method, resource string,
	body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method,
		fmt.Sprintf("%s/%s/%s", globusTransferBaseURL, globusTransferApiVersion, resource),
		body,
	)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", ep.AccessToken))
	return req, err
}

// performs a GET request on the given resource, returning the resulting
// response and error
func (ep *Endpoint) get(resource string, values url.Values) (*http.Response, error) {
	u, err := url.ParseRequestURI(globusTransferBaseURL)
	if err != nil {
		return nil, err
	}
	u.Path = fmt.Sprintf("%s/%s", globusTransferApiVersion, resource)
	u.RawQuery = values.Encode()
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("GET: %s", res))
	req, err := http.NewRequest(http.MethodGet, res, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", ep.AccessToken))
	return ep.Client.Do(req)
}

// performs a POST request on the given resource, returning the resulting
// response and error
func (ep *Endpoint) post(resource string, body io.Reader) (*http.Response, error) {
	u, err := url.ParseRequestURI(globusTransferBaseURL)
	if err != nil {
		return nil, err
	}
	u.Path = fmt.Sprintf("%s/%s", globusTransferApiVersion, resource)
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("POST: %s", res))
	req, err := http.NewRequest(http.MethodPost, res, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", ep.AccessToken))
	return ep.Client.Do(req)
}

func (ep *Endpoint) Root() string {
	return ep.root
}

func (ep *Endpoint) FilesStaged(files []frictionless.DataResource) (bool, error) {
	// find all the directories in which these files reside
	filesInDir := make(map[string][]string)
	for _, resource := range files {
		dir, file := filepath.Split(resource.Path)
		if _, found := filesInDir[dir]; !found {
			filesInDir[dir] = make([]string, 0)
		}
		filesInDir[dir] = append(filesInDir[dir], file)
	}

	// for each directory, check that its files are present
	// (https://docs.globus.org/api/transfer/file_operations/#list_directory_contents)
	for dir, files := range filesInDir {
		values := url.Values{}
		values.Add("path", "/"+dir)
		values.Add("orderby", "name ASC")
		resource := fmt.Sprintf("operation/endpoint/%s/ls", ep.Id.String())

		resp, err := ep.get(resource, values)
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return false, err
		}
		// https://docs.globus.org/api/transfer/file_operations/#dir_listing_response
		type DirListingResponse struct {
			Data []struct {
				Name string `json:"name"`
			} `json:"DATA"`
		}
		var response DirListingResponse
		err = json.Unmarshal(body, &response)
		if err != nil {
			return false, err
		}
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
	return true, nil
}

func (ep *Endpoint) Transfers() ([]uuid.UUID, error) {
	// https://docs.globus.org/api/transfer/task/#get_task_list
	values := url.Values{}
	values.Add("fields", "task_id")
	values.Add("filter", "status:ACTIVE,INACTIVE/label:DTS")
	values.Add("limit", "1000")
	values.Add("orderby", "name ASC")

	resp, err := ep.get("task_list", url.Values{})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	type TaskListResponse struct {
		Length int `json:"length"`
		Limit  int `json:"limіt"`
		Data   []struct {
			TaskId uuid.UUID `json:"task_id"`
		} `json:"DATA"`
	}
	var response TaskListResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	taskIds := make([]uuid.UUID, len(response.Data))
	for i, data := range response.Data {
		taskIds[i] = data.TaskId
	}
	return taskIds, nil
}

// https://docs.globus.org/api/transfer/task_submit/#get_submission_id
func (ep *Endpoint) getSubmissionId() (uuid.UUID, error) {
	var id uuid.UUID
	resp, err := ep.get("submission_id", url.Values{})
	if err != nil {
		return id, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return id, err
	}
	type SubmissionIdResponse struct {
		Value uuid.UUID `json:"value"`
	}
	var response SubmissionIdResponse
	err = json.Unmarshal(body, &response)
	return response.Value, err
}

// https://docs.globus.org/api/transfer/task_submit/#submit_transfer_task
// https://docs.globus.org/api/transfer/task_submit/#transfer_item_fields
func (ep *Endpoint) submitTransfer(destination endpoints.Endpoint, submissionId uuid.UUID, files []endpoints.FileTransfer) (uuid.UUID, error) {
	var xferId uuid.UUID

	type TransferItem struct {
		DataType          string `json:"DATA_TYPE"` // "transfer_item"
		SourcePath        string `json:"source_path"`
		DestinationPath   string `json:"destination_path"`
		ExternalChecksum  string `json:"external_checksum,omitempty"`
		ChecksumAlgorithm string `json:"checksum_algorithm,omitempty"`
	}
	type SubmissionRequest struct {
		DataType            string         `json:"DATA_TYPE"` // "transfer"
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
			DataType:          "transfer_item",
			SourcePath:        file.SourcePath,
			DestinationPath:   file.DestinationPath,
			ExternalChecksum:  file.Hash,
			ChecksumAlgorithm: file.HashAlgorithm,
		}
	}

	// the destination is a Globus endpoint, right?
	gDestination, ok := destination.(*Endpoint)
	if !ok {
		return xferId, fmt.Errorf("The destination is not a Globus endpoint.")
	}

	data, err := json.Marshal(SubmissionRequest{
		DataType:            "transfer",
		Id:                  submissionId.String(),
		Label:               "DTS",
		Data:                xferItems,
		DestinationEndpoint: gDestination.Id.String(),
		SourceEndpoint:      ep.Id.String(),
		SyncLevel:           3, // transfer only if checksums don't match
		VerifyChecksum:      true,
		FailOnQuotaErrors:   true,
	})
	if err != nil {
		return xferId, err
	}
	resp, err := ep.post("transfer", bytes.NewReader(data))
	if err != nil {
		return xferId, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return xferId, err
	}
	type SubmissionResponse struct {
		TaskId  uuid.UUID `json:"task_id"`
		Code    string    `json:"code"`
		Message string    `json:"message"`
	}

	var gResp SubmissionResponse
	err = json.Unmarshal(body, &gResp)
	if err != nil {
		return xferId, err
	}
	xferId = gResp.TaskId
	var zeroId uuid.UUID
	if xferId == zeroId { // trouble!
		return xferId, fmt.Errorf("%s (%s)", gResp.Message, gResp.Code)
	}
	return xferId, nil
}

func (ep *Endpoint) Transfer(destination endpoints.Endpoint, files []endpoints.FileTransfer) (uuid.UUID, error) {
	// check that all requested files are staged on this endpoint
	// (Globus does not perform this check by itself)
	requestedFiles := make([]frictionless.DataResource, len(files))
	for i, file := range files {
		requestedFiles[i].Path = file.SourcePath // only the Path field is required
	}
	staged, err := ep.FilesStaged(requestedFiles)
	if err != nil {
		return uuid.UUID{}, err
	}
	if !staged {
		return uuid.UUID{}, fmt.Errorf("The files requested for transfer are not yet staged.")
	}

	// obtain a submission ID
	submissionId, err := ep.getSubmissionId()
	if err != nil {
		return uuid.UUID{}, err
	}

	// now, submit the transfer task itself
	return ep.submitTransfer(destination, submissionId, files)
}

// mapping of Globus status code strings to DTS status codes
var statusCodesForStrings = map[string]endpoints.TransferStatusCode{
	"ACTIVE":    endpoints.TransferStatusActive,
	"INACTIVE":  endpoints.TransferStatusInactive,
	"SUCCEEDED": endpoints.TransferStatusSucceeded,
	"FAILED":    endpoints.TransferStatusFailed,
}

func (ep *Endpoint) Status(id uuid.UUID) (endpoints.TransferStatus, error) {
	resource := fmt.Sprintf("task/%s", id.String())
	resp, err := ep.get(resource, url.Values{})
	if err != nil {
		return endpoints.TransferStatus{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return endpoints.TransferStatus{}, err
	}
	type TaskResponse struct {
		Files            int    `json:"files"`
		FilesSkipped     int    `json:"files_skipped"`
		FilesTransferred int    `json:"files_transferred"`
		IsPaused         bool   `json:"is_paused"`
		Status           string `json:"status"`
		// the following fields are present only when an error occurs
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	var response TaskResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return endpoints.TransferStatus{}, err
	}
	if strings.Contains(response.Code, "ClientError") { // e.g. not found
		return endpoints.TransferStatus{}, fmt.Errorf(response.Message)
	}
	return endpoints.TransferStatus{
		Code:                statusCodesForStrings[response.Status],
		NumFiles:            response.Files,
		NumFilesSkipped:     response.FilesSkipped,
		NumFilesTransferred: response.FilesTransferred,
	}, nil
}

func (ep *Endpoint) Cancel(id uuid.UUID) error {
	// Because cancellation requests can't be honored under all circumstances,
	// this Globus call is asynchronous. Nevertheless, the Globus documentation
	// (https://docs.globus.org/api/transfer/task/#cancel_task_by_id) claims the
	// call can take up to 10 seconds before returning, which doesn't meet the
	// needs of the DTS. The possible outcomes of the call are identified with
	// these response codes:
	// 1. "Canceled", indicating that the task has been canceled
	// 2. "CancelAccepted", indicating that the cancellation request has been
	//    acknowledged but not yet processed
	// 3. "TaskComplete", indicating that the task is complete and not able to
	//    be canceled.
	//
	// To avoid a 10-second wait, we simply issue a request asynchronously and
	// settle for a "best-effort" execution, which (it seems to me) is just a less
	// elaborate framing of what Globus gives us.

	errChan := make(chan error, 1) // <-- captures immediately issued errors
	go func() {
		resource := fmt.Sprintf("task/%s/cancel", id.String())
		_, err := ep.post(resource, nil) // can take up to 10 ѕeconds!
		if err != nil {
			errChan <- err
			return
		}
		// no need to read the response--just close the error channel
		close(errChan)
	}()
	select {
	case err := <-errChan: // error received!
		return err
	case <-time.After(10 * time.Millisecond): // short timeout period
		return nil
	}
}
