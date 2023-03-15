package core

type GlobusEndpoint struct {
	User string
	URL  string
}

func (ep *GlobusEndpoint) BeginTransfer() (*Transfer, error) {
	return nil, nil
}
