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

package globus_s3_api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	ApiUrl     = "https://transfer.api.globusonline.org"
	AuthUrl    = "https://auth.globus.org/v2/oauth2/token"
	ApiVersion = "v0.10"
)

var globusDefaultScopes = []string{
	"urn:globus:auth:scope:transfer.api.globus.org:all",
}

// Endpoint contains the information needed to access a Globus endpoint.
type Endpoint struct {
	EndpointID uuid.UUID  `json:"endpoint_id"`
	Session    Session    `json:"session"`
	Settings   Settings   `json:"settings"`
	Config	   Config     `json:"config"`
}

// Configuration information for creating a new Globus endpoint.
type Config struct {
	EndpointID   uuid.UUID `json:"endpoint_id"`
	ClientId     uuid.UUID `json:"client_id"`
	ClientSecret string    `json:"client_secret"`
	Scopes       []string  `json:"scopes"`
}

// Creates a new Globus endpoint from the provided config.
// Authenticates the client and retrieves the endpoint settings.
func NewEndpoint(config Config) (Endpoint, error) {
	if config.EndpointID == uuid.Nil {
		return Endpoint{}, fmt.Errorf("endpoint ID not set in config")
	}
	session, err := authenticateClient(config)
	if err != nil {
		return Endpoint{}, fmt.Errorf("failed to authenticate Globus client: %w", err)
	}
	endpoint := Endpoint{
		EndpointID: config.EndpointID,
		Session:    session,
		Config:     config,
	}
	settings, err := endpoint.getSettings()
	if err != nil {
		return Endpoint{}, fmt.Errorf("failed to get endpoint settings: %w", err)
	}
	endpoint.Settings = settings
	return endpoint, nil
}

// Handles GET requests by forwarding them to the Globus API.
func (endpoint *Endpoint) HandleGetRequest(path string) ([]byte, error) {
	resource := fmt.Sprintf("operation/endpoint/%s/ls", endpoint.EndpointID.String())
	values := url.Values{}
	values.Set("path", "/"+path)
	values.Set("orderby", "name ASC")
	data, err := endpoint.get(resource, values)
	if err != nil {
		return nil, err
	}
	if responseIsError(data) {
		return nil, fmt.Errorf("error response from Globus API: %s", string(data))
	}
	return data, nil
}


//-----------
// Internals
//-----------

// Globus session information
type Session struct {
	AccessToken string    `json:"access_token"`
	Expires     time.Time `json:"expires"`
}

// Endpoint settings
type Settings struct {
	DisableVerify bool `json:"disable_verify"` // true if checksums are not available
	ForceVerify   bool `json:"force_verify"`   // true to force checksum verification
}

// returns true if a Globus response body matches an error
func responseIsError(body []byte) bool {
	bodyStr := string(body)
	return strings.Contains(bodyStr, "\"code\"") &&
		!strings.Contains(bodyStr, "\"code\": \"Accepted\"") &&
		strings.Contains(string(body), "\"message\"")
}

// Helper function to send requests to the Globus API
func (endpoint *Endpoint) sendRequest(request *http.Request) ([]byte, error) {
	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status code %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// Performs a GET request to the Globus API.
//
// If the access token is expired, it will attempt to re-authenticate
// the client using the provided config.
func (endpoint *Endpoint) get(resource string, values url.Values) ([]byte, error) {
	if time.Now().After(endpoint.Session.Expires) {
		newSession, err := authenticateClient(endpoint.Config)
		if err != nil {
			return nil, fmt.Errorf("failed to re-authenticate Globus client: %w", err)
		}
		endpoint.Session = newSession
	}
	uri, err := url.ParseRequestURI(ApiUrl)
	if err != nil {
		return nil, err
	}
	uri.Path = fmt.Sprintf("/%s/%s", ApiVersion, resource)
	uri.RawQuery = values.Encode()

	req, err := http.NewRequest(http.MethodGet, uri.String(), http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", endpoint.Session.AccessToken))

	return endpoint.sendRequest(req)
}

// Authenticates a Globus client and returns an access token.
func authenticateClient(config Config) (Session, error) {
	globusScopes := config.Scopes
	if len(globusScopes) == 0 {
		globusScopes = globusDefaultScopes
	}
	data := url.Values{}
	data.Set("scope", strings.Join(globusScopes, " "))
    data.Set("grant_type", "client_credentials")
	req, err := http.NewRequest(http.MethodPost, AuthUrl, strings.NewReader(data.Encode()))
	if err != nil {
		return Session{}, err
	}
	req.SetBasicAuth(config.ClientId.String(), config.ClientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return Session{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Session{}, fmt.Errorf("authentication failed with status code %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Session{}, err
	}
    var authResponse struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	err = json.Unmarshal(body, &authResponse)
	if err != nil {
		return Session{}, err
	}
	if authResponse.AccessToken == "" {
		return Session{}, fmt.Errorf("authentication response missing access token")
	}
	expires := time.Now().Add(time.Duration(authResponse.ExpiresIn) * time.Second)
	
	return Session{
		AccessToken: authResponse.AccessToken,
		Expires:     expires,
	}, nil
}

// Get endpoint settings from the Globus API.
func (endpoint *Endpoint) getSettings() (Settings, error) {
	data, err := endpoint.get(fmt.Sprintf("endpoint/%s", endpoint.EndpointID.String()), url.Values{})
	if err != nil {
		return Settings{}, err
	}
	if responseIsError(data) {
		return Settings{}, fmt.Errorf("error response from Globus API: %s", string(data))
	}
	var resp Settings
	err = json.Unmarshal(data, &resp)
	if err != nil {
		return Settings{}, err
	}
	return resp, nil
}



