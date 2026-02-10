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
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	ApiUrl     = "https://transfer.api.globusonline.org"
	AuthUrl    = "https://auth.globus.org/v2/oauth2/token"
	AuthorizeUrl = "https://auth.globus.org/v2/oauth2/authorize"
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
	RedirectUri  string    `json:"redirect_uri"`   // For OAuth flow
	UserToken    string    `json:"user_token"`     // [optional] Pre-obtained user token
	UserTokenExpires time.Time `json:"user_token_expires"` // [optional] Expiration time of user token
}

// Creates a new Globus endpoint from the provided config.
// Authenticates the client and retrieves the endpoint settings.
func NewEndpoint(config Config) (Endpoint, error) {
	if config.EndpointID == uuid.Nil {
		return Endpoint{}, fmt.Errorf("endpoint ID not set in config")
	}

	var session Session
	var err error

	// if user token is provided, use it
	if config.UserToken != "" {
		if config.UserTokenExpires.Before(time.Now()) {
			return Endpoint{}, fmt.Errorf("provided user token is expired")
		}
		session = Session{
			AccessToken: config.UserToken,
			Expires:     config.UserTokenExpires,
		}
	} else {
	    // otherwise, authenticate the client (won't work for HTTPS file access)
		session, err = authenticateClient(config)
		if err != nil {
			return Endpoint{}, fmt.Errorf("failed to authenticate Globus client: %w", err)
		}
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

// Replaces the session with a new one.
func (e *Endpoint) WithSession(session Session) Endpoint {
	return Endpoint{
		EndpointID: e.EndpointID,
		Session:    session,
		Settings:   e.Settings,
		Config:     e.Config,
	}
}

// GetAuthorizationUrl returns the URL to redirect users to for OAuth authorization.
func (e Endpoint) GetAuthorizationUrl() string {
	scopes := []string{
		"urn:globus:auth:scope:transfer.api.globus.org:all",
		getCollectionScope(e.EndpointID),
	}

	values := url.Values{}
	values.Set("client_id", e.Config.ClientId.String())
	values.Set("redirect_uri", e.Config.RedirectUri)
	values.Set("scope", strings.Join(scopes, " "))
	values.Set("response_type", "code")
	values.Set("access_type", "offline") // Request refresh token
	
	uri, _ := url.ParseRequestURI(AuthorizeUrl)
	uri.RawQuery = values.Encode()
	return uri.String()
}

// ExchangeAuthorizationCode exchanges an OAuth authorization code for an access token.
func (e Endpoint) ExchangeAuthorizationCode(code string) (Session, error) {
	data := url.Values{}
	data.Set("grant_type", "authorization_code")
	data.Set("code", code)
	data.Set("redirect_uri", e.Config.RedirectUri)

	req, err := http.NewRequest(http.MethodPost, AuthUrl, strings.NewReader(data.Encode()))
	if err != nil {
		return Session{}, err
	}
	req.SetBasicAuth(e.Config.ClientId.String(), e.Config.ClientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return Session{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return Session{}, fmt.Errorf("token exchange failed with status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Session{}, err
	}
	var tokenResponse struct {
		AccessToken string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	err = json.Unmarshal(body, &tokenResponse)
	if err != nil {
		return Session{}, err
	}

	expires := time.Now().Add(time.Duration(tokenResponse.ExpiresIn) * time.Second)
	return Session{
		AccessToken: tokenResponse.AccessToken,
		RefreshToken: tokenResponse.RefreshToken,
		Expires:     expires,
	}, nil
}

// RefreshToken refreshes an expired access token using the refresh token.
func (e Endpoint) RefreshToken() (Session, error) {
	data := url.Values{}
	data.Set("grant_type", "refresh_token")
	data.Set("refresh_token", e.Session.RefreshToken)

	req, err := http.NewRequest(http.MethodPost, AuthUrl, strings.NewReader(data.Encode()))
	if err != nil {
		return Session{}, err
	}
	req.SetBasicAuth(e.Config.ClientId.String(), e.Config.ClientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return Session{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return Session{}, fmt.Errorf("token refresh failed with status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Session{}, err
	}
	var tokenResponse struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	err = json.Unmarshal(body, &tokenResponse)
	if err != nil {
		return Session{}, err
	}

	expires := time.Now().Add(time.Duration(tokenResponse.ExpiresIn) * time.Second)
	return Session{
		AccessToken: tokenResponse.AccessToken,
		RefreshToken: e.Session.RefreshToken,
		Expires:     expires,
	}, nil
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

// Returns a direct HTTPS URL for accessing a file on the Globus endpoint.
func (endpoint *Endpoint) GetHttpsUrl(path string) (string, error) {
	if endpoint.Settings.HttpsServer == "" {
		return "", fmt.Errorf("HTTPS server not available for this endpoint")
	}
	httpsUrl, err := url.JoinPath(endpoint.Settings.HttpsServer, path)
	if err != nil {
		return "", fmt.Errorf("failed to join HTTPS server URL and path: %w", err)
	}
	return httpsUrl, nil
}

// Returns whether the Globus endpoint supports HTTPS access.
func (endpoint *Endpoint) SupportsHttps() bool {
	return endpoint.Settings.HttpsServer != ""
}

// Proxies content from the given HTTPS URL to the client
func (endpoint *Endpoint) ProxyHttpsContent(w http.ResponseWriter, r *http.Request, httpsUrl string, path string) {
	log.Printf("Proxying content from HTTPS URL: %s\n", httpsUrl)

    // Create request to HTTPS URL
	req, err := http.NewRequest(r.Method, httpsUrl, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create request to HTTPS URL: %v", err), http.StatusInternalServerError)
		return
	}

	// Copy headers from original request
	for name, values := range r.Header {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}

	// Add Globus authentication
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", endpoint.Session.AccessToken))

	// Set long timeout for large file transfers
	client := &http.Client{
		Timeout: 30 * time.Minute,
	}

	// Send request to HTTPS URL
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error retrieving content from HTTPS URL: %v\n", err)
		http.Error(w, fmt.Sprintf("Failed to retrieve content from HTTPS URL: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	log.Printf("Received response from HTTPS URL with status code: %d %s\n", resp.StatusCode, resp.Status)

	// check for errors
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("Error response body: %s\n", string(body))
		http.Error(w, fmt.Sprintf("Error response from HTTPS URL: %s", string(body)), resp.StatusCode)
		return
	}

	// Copy response headers to client
	for name, value := range resp.Header {
		w.Header()[name] = value
	}

	// Override/Add Headers for S3 compatibility
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", url.PathEscape(path)))

	// Generate a simple ETag
	etag := fmt.Sprintf("\"%x-%x\"", time.Now().Unix(), resp.ContentLength)
	w.Header().Set("ETag", etag)

	// Write status code to client
	w.WriteHeader(resp.StatusCode)

	// Stream content to client
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Printf("Error streaming content to client: %v\n", err)
	}
}

//-----------
// Internals
//-----------

// Globus session information
type Session struct {
	AccessToken string    `json:"access_token"`
	RefreshToken string   `json:"refresh_token,omitempty"`
	Expires     time.Time `json:"expires"`
}

// Endpoint settings
type Settings struct {
	DisableVerify bool   `json:"disable_verify"` // true if checksums are not available
	ForceVerify   bool   `json:"force_verify"`   // true to force checksum verification
	HttpsServer   string `json:"https_server"`   // HTTPS server URL
}

// returns true if a Globus response body matches an error
func responseIsError(body []byte) bool {
	bodyStr := string(body)
	return strings.Contains(bodyStr, "\"code\"") &&
		!strings.Contains(bodyStr, "\"code\": \"Accepted\"") &&
		strings.Contains(string(body), "\"message\"")
}

// returns the data access scope for a specific collection
func getCollectionScope(collectionId uuid.UUID) string {
	return fmt.Sprintf("https://auth.globus.org/scopes/%s/data_access", collectionId.String())
}

// Helper function to send requests to the Globus API
func sendRequest(request *http.Request) ([]byte, error) {
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

	return sendRequest(req)
}

// getEndpointScopes returns available scopes for the endpoint.
func getEndpointScopes(endpointID uuid.UUID, accessToken string) ([]string, error) {
    resource := fmt.Sprintf("endpoint/%s", endpointID.String())

	uri, err := url.ParseRequestURI(ApiUrl)
	if err != nil {
		return nil, err
	}
	uri.Path = fmt.Sprintf("/%s/%s", ApiVersion, resource)
	uri.RawQuery = url.Values{}.Encode()

	req, err := http.NewRequest(http.MethodGet, uri.String(), http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", accessToken))

	data, err := sendRequest(req)
	if err != nil {
		return nil, err
	}
	if responseIsError(data) {
		return nil, fmt.Errorf("error response from Globus API: %s", string(data))
	}

	var endpointDetails struct {
		Id                string   `json:"id"`
        DisplayName       string   `json:"display_name"`
        EntityType        string   `json:"entity_type"`
        SubscriptionId    string   `json:"subscription_id"`
        DataAccessScopes  []string `json:"data_access_scopes"`
        GCSManagerUrl     string   `json:"gcs_manager_url"`
        HttpsServer       string   `json:"https_server"`
	}

	err = json.Unmarshal(data, &endpointDetails)
	if err != nil {
		return nil, err
	}

	log.Printf("Endpoint details for %s:\n", endpointID.String())
	log.Printf("  Display Name: %s\n", endpointDetails.DisplayName)
	log.Printf("  Entity Type: %s\n", endpointDetails.EntityType)
	log.Printf("  Subscription ID: %s\n", endpointDetails.SubscriptionId)
	log.Printf("  Data Access Scopes: %v\n", endpointDetails.DataAccessScopes)
	log.Printf("  GCS Manager URL: %s\n", endpointDetails.GCSManagerUrl)
	log.Printf("  HTTPS Server: %s\n", endpointDetails.HttpsServer)

	if endpointDetails.SubscriptionId != "" {
		log.Printf("  Subscription id %s (this appears to be a mapped collection)\n", endpointDetails.SubscriptionId)
	}

	scopes := endpointDetails.DataAccessScopes
	if len(scopes) == 0 && endpointDetails.SubscriptionId != "" {
		// if no data access scopes are listed, but there is a subscription id,
		// assume the collection scope is needed
		mappedCollectionScope := fmt.Sprintf("https://auth.globus.org/scopes/%s/data_access", endpointDetails.SubscriptionId)
		scopes = []string{mappedCollectionScope}
	}
	
	return scopes, nil
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
		RefreshToken string `json:"refresh_token"`
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

    // Now, do it all again with the collection scopes
	collectionScopes, err := getEndpointScopes(config.EndpointID, authResponse.AccessToken)
	if err != nil {
		return Session{}, fmt.Errorf("failed to get endpoint scopes: %w", err)
	}
	if len(collectionScopes) == 0 {
		return Session{
			AccessToken: authResponse.AccessToken,
			RefreshToken: authResponse.RefreshToken,
			Expires:     expires,
		}, nil
	}
	globusScopes = append(globusScopes, collectionScopes...)

	data = url.Values{}
	data.Set("scope", strings.Join(globusScopes, " "))
	data.Set("grant_type", "client_credentials")
	req, err = http.NewRequest(http.MethodPost, AuthUrl, strings.NewReader(data.Encode()))
	if err != nil {
		return Session{}, err
	}
	req.SetBasicAuth(config.ClientId.String(), config.ClientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err = client.Do(req)
	if err != nil {
		return Session{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Session{}, fmt.Errorf("authentication failed with status code %d", resp.StatusCode)
	}

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return Session{}, err
	}
	err = json.Unmarshal(body, &authResponse)
	if err != nil {
		return Session{}, err
	}
	if authResponse.AccessToken == "" {
		return Session{}, fmt.Errorf("authentication response missing access token")
	}
	expires = time.Now().Add(time.Duration(authResponse.ExpiresIn) * time.Second)

	return Session{
		AccessToken: authResponse.AccessToken,
		RefreshToken: authResponse.RefreshToken,
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



