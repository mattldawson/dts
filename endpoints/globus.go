package endpoints

import (
	"github.com/google/uuid"
)

type GlobusEndpoint struct {
	User string
	URL  string
}

func (ep *GlobusEndpoint) FilesStaged(filePaths []string) bool {
	return false
}

func (ep *GlobusEndpoint) Transfer(dst Endpoint, srcPaths, dstPaths []string) (uuid.UUID, error) {
	return uuid.New(), nil
}

func (ep *GlobusEndpoint) Status(id uuid.UUID) TransferStatus {
	return Unknown
}
