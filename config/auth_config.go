package config

import (
	"github.com/google/uuid"
)

type authConfig struct {
	// the client ID (uuid)
	ClientId uuid.UUID `yaml:"client_id"`
	// the client secret used to obtain API access tokens or make requests
	// DO NOT STORE THIS IN A CONFIG FILE! Use an environment variable instead
	ClientSecret string `yaml:"client_secret"`
}
