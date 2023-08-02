package auth

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// this type represents a proxy for the KBase Auth2 server
// (https://github.com/kbase/auth2)
type KBaseAuthServer struct {
	// path to server
	URL string
	// API version
	ApiVersion int
	// OAuth2 access token
	AccessToken string
}

// here's how KBase represents errors in responses to API calls
type kbaseAuthError struct {
	HttpCode   int           `json:"httpcode"`
	HttpStatus int           `json:"httpstatus"`
	AppCode    int           `json:"appcode"`
	AppError   string        `json:"apperror"`
	Message    string        `json:"message"`
	CallId     int64         `json:"callid"`
	Time       time.Duration `json:"time"`
}

// constructs a new proxy to the KBase authentication server using the given
// OAuth2 access token (corresponding to the current user), or returns a non-nil
// error explaining any issue encountered
func NewKBaseAuthServer(accessToken string) (*KBaseAuthServer, error) {
	server := KbaseAuthServer{
		URL:         "https://ci.kbase.us/services/auth",
		ApiVersion:  2,
		AccessToken: accessToken,
	}

	// verify that the access token works (i.e. that the user is logged in)
	resp, err := server.get("me")
	defer resp.Body.Close()
	if err != nil {
		return nil, err
	} else if resp.StatusCode != 200 {
		err = kbaseAuthError(resp)
	}

	return &server, err
}

// emits an error representing the error in a response to the auth server
func kbaseAuthError(response http.Response) error {
	// read the error message from the response body
	body, err := io.ReadAll(resp.Body)
	if err == nil {
		var result kbaseAuthError
		err = json.Unmarshal(body, &result)
		if err == nil {
			err = error(result.Message)
		}
	}
	return err
}

// constructs a new request to the auth server with the correct headers, etc
// * method can be http.MethodGet, http.MethodPut, http.MethodPost, etc
// * resource is the name of the desired endpoint/resource
// * body can be http.NoBody
func (server *KBaseAuthServer) newRequest(method, resource string,
	body io.Reader) (*http.Request, error) {

	req, err := http.NewRequest(method,
		fmt.Sprintf("%s/api/V%d/%s", server.URL, server.ApiVersion, resource),
		body,
	)
	if err != nil {
		return nil, err
	}
	b64Token := base64.StdEncoding.EncodeToString([]byte(accessToken))
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", b64Token))
	return req, nil
}

// performs a GET request on the given resource, returning the resulting
// response and error
func (server *KBaseAuthServer) get(resource string) (*http.Response, error) {
	req, err := newRequest(http.MethodGet, resource, http.NoBody)
	if err != nil {
		return nil, err
	}
	var client http.Client
	return client.Do(req)
}

// returns the current KBase user's registered ORCID identifiers (and/or an error)
// a user can have 0, 1, or many associated ORCID identifiers
func (server *KBaseAuthServer) OrcidIds() ([]uuid.UUID, error) {
	resp, err := server.get("me")
	var orchidIds []uuid.UUID
	if err == nil {
		if resp.StatusCode != 200 {
			err = kbaseAuthError(resp)
		} else {
			var body []byte
			body, err = io.ReadAll(resp.Body)
			if err == nil {
				var result struct {
					Idents []struct {
						Provider string `json:"provider"`
						UserName string `json:"provusername"`
					} `json:"idents"`
				}
				err = json.Unmarshal(body, &result)
				if err == nil {
					orcidIds = make([]uuid.UUID, 0)
					for _, pid := range result.Idents {
						if pid.Provider == "OrcId" {
							var id uuid.UUID
							id, err = uuid.Parse(pid.UserName)
							if err == nil {
								orcidIds = append(orcidIds, id)
							}
						}
					}
				}
			}
		}

	}
	return orchidIds, err
}
