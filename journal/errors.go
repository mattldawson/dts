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

package journal

import (
	"fmt"

	"github.com/google/uuid"
)

// indicates that the journal is not open and cannot respond to the given request
type NotOpenError struct {
}

func (e NotOpenError) Error() string {
	return "The transfer journal is not open for reading or writing."
}

// indicates that the journal cannot be opened
type CantOpenError struct {
	Message string
}

func (e CantOpenError) Error() string {
	return fmt.Sprintf("Can't open transfer journal: %s", e.Message)
}

// indicates that the journal cannot be closed
type CantCloseError struct {
	Message string
}

func (e CantCloseError) Error() string {
	return fmt.Sprintf("Can't close transfer journal: %s", e.Message)
}

// indicates that a transfer could not be recorded
type NewRecordError struct {
	Id      uuid.UUID
	Message string
}

func (e NewRecordError) Error() string {
	return fmt.Sprintf("Could not create a new transfer record with ID %s: %s", e.Id.String(), e.Message)
}

// indicates that the transfer with the given ID has no record in the journal
type RecordNotFoundError struct {
	Id uuid.UUID
}

func (e RecordNotFoundError) Error() string {
	return fmt.Sprintf("No transfer record was found with ID %s", e.Id.String())
}

// indicates that the transfer record with the given ID has invalid data
type InvalidRecordError struct {
	Id      uuid.UUID
	Message string
}

func (e InvalidRecordError) Error() string {
	return fmt.Sprintf("Invalid transfer record for ID %s: %s!", e.Id.String(), e.Message)
}
