package config

import (
	"github.com/google/uuid"
)

type globusConfig struct {
	// authentication/authorization data
	Auth struct {
		// the client ID (uuid)
		ClientId uuid.UUID `yaml:"client_id"`
		// the client secret used to obtain API access tokens
		// DO NOT STORE THIS IN A CONFIG FILE! Use an environment variable instead
		ClientSecret string `yaml:"client_secret"`
	} `yaml:"auth"`
	// endpoints
	Endpoints map[string]struct {
		// descriptive name of the Globus endpoint
		Name string `yaml:"name"`
		// the Globus endpoint ID (uuid)
		Id uuid.UUID `yaml:"id"`
	} `yaml:"endpoints"`
}
