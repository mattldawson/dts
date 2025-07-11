package config

import (
	"github.com/google/uuid"
)

type endpointConfig struct {
	// descriptive name of the endpoint
	Name string `yaml:"name"`
	// the endpoint ID (uuid)
	Id uuid.UUID `yaml:"id"`
	// the name of the provider (e.g. "globus")
	Provider string `yaml:"provider"`
	// the name of the credential to use with this endpoint
	Credential string `yaml:"credential"`
	// root directory for filesystem access (optional)
	Root string `yaml:"root,omitempty"`
}
