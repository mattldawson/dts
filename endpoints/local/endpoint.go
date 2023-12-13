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

package local

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/core"
)

// This file implements an endpoint that moves files around on a local
// file system. It's used only for testing.

type Endpoint struct {
	// descriptive endpoint name (obtained from config)
	Name string
	// endpoint UUID (obtained from config)
	Id uuid.UUID
	// root directory for endpoint (default: current working directory)
	root string
	// transfers in progress
	Xfers map[uuid.UUID]core.TransferStatus
}

// creates a new local endpoint using the information supplied in the
// DTS configuration file under the given endpoint name
func NewEndpoint(endpointName string) (core.Endpoint, error) {
	epConfig, found := config.Endpoints[endpointName]
	if !found {
		return nil, fmt.Errorf("'%s' is not an endpoint", endpointName)
	}
	if epConfig.Provider != "local" {
		return nil, fmt.Errorf("'%s' is not a local endpoint", endpointName)
	}
	if epConfig.Root == "" {
		return nil, fmt.Errorf("'%s' requires a root directory to be specified", endpointName)
	}

	ep := &Endpoint{
		Name:  epConfig.Name,
		Id:    epConfig.Id,
		Xfers: make(map[uuid.UUID]core.TransferStatus),
	}
	err := ep.setRoot(epConfig.Root)
	return ep, err
}

// sets the root directory for the local endpoint after checking that it exists
func (ep *Endpoint) setRoot(dir string) error {
	_, err := os.Stat(dir)
	if err == nil {
		ep.root = dir
	}
	return err
}

func (ep *Endpoint) Root() string {
	return ep.root
}

func (ep *Endpoint) FilesStaged(files []core.DataResource) (bool, error) {
	for _, resource := range files {
		absPath := filepath.Join(ep.root, resource.Path)
		_, err := os.Stat(absPath)
		if err != nil {
			return false, nil
		}
	}
	return true, nil
}

func (ep *Endpoint) Transfers() ([]uuid.UUID, error) {
	xfers := make([]uuid.UUID, 0)
	for xferId, xferStatus := range ep.Xfers {
		switch xferStatus.Code {
		case core.TransferStatusSucceeded, core.TransferStatusFailed:
		default:
			xfers = append(xfers, xferId)
		}
	}
	return xfers, nil
}

// implements asynchronous local file transfers and validation
func (ep *Endpoint) transferFiles(xferId uuid.UUID, dest core.Endpoint, files []core.FileTransfer) {
	for _, file := range files {
		sourcePath := filepath.Join(ep.Root(), file.SourcePath)
		destPath := filepath.Join(dest.Root(), file.DestinationPath)

		// create the destination directory if needed
		sourceDir := filepath.Dir(sourcePath)
		sourceDirInfo, err := os.Stat(sourceDir)
		destDir := filepath.Dir(destPath)
		_, err = os.Stat(destDir)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				os.MkdirAll(destDir, sourceDirInfo.Mode())
			} else {
				status := ep.Xfers[xferId]
				status.Code = core.TransferStatusFailed
				ep.Xfers[xferId] = status
				break
			}
		}

		// copy the file into place
		sourceInfo, err := os.Stat(sourcePath)
		if err == nil {
			data, err := os.ReadFile(sourcePath)
			if err == nil {
				err = os.WriteFile(destPath, data, sourceInfo.Mode())
			}
		}
		status := ep.Xfers[xferId]
		if err == nil {
			status.NumFilesTransferred++
			ep.Xfers[xferId] = status
		} else {
			status.Code = core.TransferStatusFailed
			ep.Xfers[xferId] = status
			break
		}
	}
	status := ep.Xfers[xferId]
	status.Code = core.TransferStatusSucceeded
	ep.Xfers[xferId] = status
}

func (ep *Endpoint) Transfer(dst core.Endpoint, files []core.FileTransfer) (uuid.UUID, error) {
	_, ok := dst.(*Endpoint)
	if !ok {
		return uuid.UUID{}, fmt.Errorf("Destination endpoint must be local!")
	}

	// first, we check that all requested files are staged on this endpoint
	requestedFiles := make([]core.DataResource, len(files))
	for i, file := range files {
		requestedFiles[i].Path = file.SourcePath // only the Path field is required
	}
	staged, err := ep.FilesStaged(requestedFiles)
	if err == nil && staged {
		// assign a UUID to the transfer and set it going
		xferId := uuid.New()
		ep.Xfers[xferId] = core.TransferStatus{
			Code:                core.TransferStatusActive,
			NumFiles:            len(files),
			NumFilesTransferred: 0,
		}
		go ep.transferFiles(xferId, dst, files)
		return xferId, nil
	} else {
		if err == nil {
			err = fmt.Errorf("The files requested for transfer are not yet staged.")
		}
		return uuid.UUID{}, err
	}
}

func (ep *Endpoint) Status(id uuid.UUID) (core.TransferStatus, error) {
	if xferStatus, found := ep.Xfers[id]; found {
		return xferStatus, nil
	} else {
		return core.TransferStatus{
			Code: core.TransferStatusUnknown,
		}, fmt.Errorf("Transfer %s not found!", id.String())
	}
}

func (ep *Endpoint) Cancel(id uuid.UUID) error {
	return fmt.Errorf("Local transfers cannot be canceled!")
}
