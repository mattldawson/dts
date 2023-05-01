package endpoints

import (
	"dts/config"
	"dts/core"
	"dts/endpoints/globus"
)

// creates an endpoint based on the configured type
func NewEndpoint(endpointName string) (core.Endpoint, error) {
	_, found := config.Globus.Endpoints[endpointName]
	if found {
		return globus.NewEndpoint(endpointName)
	}
	return nil, fmt.Errorf("Invalid endpoint: %s", endpointName)
}
