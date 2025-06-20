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

package endpoints

import (
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
)

// A custom endpoint specification encoded in a string of the form "<provider>:<id>:<credential>",
// where
// * provider is the name of a tranport service (e.g. "globus") as in the config file
// * id is a corresponding identifier for the destination (e.g. a UUID for a Globus share)
// * credential is the name of a credential stored in the config file
type CustomSpec struct {
	Provider, Id, Credential string
}

// parses the string into a CustomSpec, returning an error if the spec is invalid
func ParseCustomSpec(s string) (CustomSpec, error) {
	terms := strings.Split(s, ":")
	if len(terms) != 3 {
		return CustomSpec{}, &InvalidCustomSpecError{
			String:  s,
			Message: "does not match form <provider>:<id>:<credential>",
		}
	}
	customSpec := CustomSpec{
		Provider:   terms[0],
		Id:         terms[1],
		Credential: terms[2],
	}

	// for now, custom destinations must be Globus shares
	if customSpec.Provider != "globus" {
		return CustomSpec{}, &InvalidCustomSpecError{
			String:  s,
			Message: fmt.Sprintf("invalid provider: %s", customSpec.Provider),
		}
	}

	// Globus shares are identified by UUIDs
	_, err := uuid.Parse(customSpec.Id)
	if err != nil {
		return CustomSpec{}, &InvalidCustomSpecError{
			String:  s,
			Message: fmt.Sprintf("invalid ID: %s", customSpec.Id),
		}
	}

	// do we have a valid credential?
	_, validCredential := config.Credentials[customSpec.Credential]
	if !validCredential {
		return CustomSpec{}, &InvalidCustomSpecError{
			String:  s,
			Message: fmt.Sprintf("invalid credential: %s", customSpec.Credential),
		}
	}

	return customSpec, nil
}
