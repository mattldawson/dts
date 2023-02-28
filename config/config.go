package config

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/yaml.v3"

	"dts/core"
)

// A type with service configuration parameters.
type serviceConfig struct {
	// Port on which the service listens‥
	Port int `json:"port" yaml:"port"`
	// Maximum number of allowed incoming connections.
	MaxConnections int `json:"maxConnections" yaml:"maxConnections"`
}

// Global config variables
var Service serviceConfig
var Endpoints map[string]core.Endpoint
var Databases map[string]core.Database
var MessageQueues map[string]core.MessageQueue

// This struct performs the unmarshalling from the YAML config file and then
// copies its fields to the globals above.
type configFile struct {
	Service serviceConfig `yaml:"service"`
	Endpoints map[string]core.Endpoint `yaml:"endpoints"`
  Databases map[string]core.Database `yaml:"databases"`
  MessageQueues map[string]core.MessageQueue `yaml:"message_queues"`
}

// This helper locates and reads a configuration file, returning an error
// indicating success or failure. All environment variables of the form
// ${ENV_VAR} are expanded.
func readConfig(bytes []byte) error {
	// Before we do anything else, expand any provided environment variables.
	bytes = []byte(os.ExpandEnv(string(bytes)))

	var conf configFile
	conf.Service.Port = 8080
	conf.Service.MaxConnections = 100
	err := yaml.Unmarshal(bytes, &conf)
	if err != nil {
		log.Printf("Couldn't parse configuration data: %s\n", err)
		return err
	}

	// Copy the config data into place.
	Service = conf.Service
	Stores = make([]string, len(conf.Stores))
	copy(Stores, conf.Stores)

	return err
}

// This helper validates the given service parameters, returning an
// error indicating success or failure.
func validateServiceParameters(params serviceConfig) error {
	if params.Port < 0 || params.Port > 65535 {
		return fmt.Errorf("Invalid port: %d (must be 0-65535)", params.Port)
	}
	if params.MaxConnections <= 0 {
		return fmt.Errorf("Invalid maxConnections: %d (must be positive)",
			params.MaxConnections)
	}
	return nil
}

// This helper validates the given configfile, returning an error that indicates
// success or failure.
func validateConfig() error {
	err := validateServiceParameters(Service)
	if err != nil {
		return err
	}
	// Were we given any data stores?
	if len(Stores) == 0 {
		return fmt.Errorf("No data stores were provided!")
	}
	// Make sure each data store exists and that there are no duplicates.
	storesFound := make(map[string]bool)
	for _, store := range Stores {
		_, found := storesFound[store]
		if found {
			return fmt.Errorf("Duplicate store found: %s", store)
		} else {
			_, err := os.Stat(store)
			if err != nil {
				return err
			}
			storesFound[store] = true
		}
	}
	return nil
}

// Initializes the ID mapping service configuration using the given YAML byte
// data.
func Init(yamlData []byte) error {
	// Perform core initialization.
	err := core.Init()
	if err != nil {
		return err
	}

	// Read the configuration from our YAML file.
	err = readConfig(yamlData)
	if err != nil {
		return err
	}

	// Validate the configuration.
	err = validateConfig()
	return err
}
