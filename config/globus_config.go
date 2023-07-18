package config

import (
	"github.com/google/uuid"
)

type globusConfig struct {
	// authentication/authorization data (client secret used to request access token)
	Auth authConfig `yaml:"auth"`
	// endpoints
	Endpoints map[string]struct {
		// descriptive name of the Globus endpoint
		Name string `yaml:"name"`
		// the Globus endpoint ID (uuid)
		Id uuid.UUID `yaml:"id"`
	} `yaml:"endpoints"`
}
