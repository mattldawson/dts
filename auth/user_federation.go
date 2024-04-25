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

package auth

import (
	"fmt"
)

//=======================
// KBase user federation
//=======================

// Because the DTS uses KBase's auth server for its own authentication, we can
// create and maintain an ORCID -> username mapping that stores entries for all
// users who have made requests to the DTS. This prevents us from having to
// rely on a secondary data source for this information.

var kbaseUserFederationStarted = false
var kbaseOrcidChan chan string        // passes ORCIDs
var kbaseOrcidUserChan chan [2]string // passes (ORCIDs, username) pairs
var kbaseUserChan chan string         // passes usernames
var kbaseErrorChan chan error         // passes errors

// This goroutine maintains a mapping or ORCID IDS to local KBase users,
// fielding requests to update and retrieve usernames by ORCID ID.
func kbaseUserFederation() {

	// mapping of ORCID IDs to local KBase users
	kbaseUserTable := make(map[string]string)

	for {
		select {
		case orcidAndUsername := <-kbaseOrcidUserChan: // setting username for orcid
			if username, found := kbaseUserTable[orcidAndUsername[0]]; found {
				if username != orcidAndUsername[1] {
					kbaseErrorChan <- fmt.Errorf("KBase user mismatch for ORCID %s!", orcidAndUsername[0])
				}
			} else {
				kbaseUserTable[orcidAndUsername[0]] = orcidAndUsername[1]
				kbaseErrorChan <- nil
			}
		case orcid := <-kbaseOrcidChan: // fetching username for orcid
			if username, found := kbaseUserTable[orcid]; found {
				kbaseUserChan <- username
			} else {
				kbaseErrorChan <- fmt.Errorf("KBase user not found for ORCID %s!", orcid)
			}
		}
	}
}

// associates the given KBase username with the given ORCID ID
func SetKBaseLocalUsernameForOrcid(orcid, username string) error {
	if !kbaseUserFederationStarted {
		// fire it up!
		kbaseUserFederationStarted = true
		kbaseOrcidChan = make(chan string, 32)
		kbaseOrcidUserChan = make(chan [2]string, 32)
		kbaseUserChan = make(chan string, 32)
		kbaseErrorChan = make(chan error, 32)
		go kbaseUserFederation()
	}
	var err error
	kbaseOrcidUserChan <- [2]string{orcid, username}
	select {
	case err = <-kbaseErrorChan:
	default:
	}
	return err
}

// returns the local KBase username associated with the given ORCID ID
func KBaseLocalUsernameForOrcid(orcid string) (string, error) {
	if !kbaseUserFederationStarted { // no one's logged in!
		return "", fmt.Errorf("KBase federated user table not available!")
	}
	var username string
	var err error
	kbaseOrcidChan <- orcid
	select {
	case username = <-kbaseUserChan:
	case err = <-kbaseErrorChan:
	}
	return username, err
}
