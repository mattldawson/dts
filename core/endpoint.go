package core

import (
	"dts/config"
)

type Endpoint interface {
	BeginTransfer() (*Transfer, error)
}

// creates an endpoint based on the configured type
func NewEndpoint(endpointName string) (Endpoint, error) {
	epConfig := config.Endpoints[endpointName]
	if len(epConfig.Globus.URL) > 0 {
		return &GlobusEndpoint{
			User: epConfig.Globus.User,
			URL:  epConfig.Globus.URL,
		}, nil
	}
	return nil, nil
}
