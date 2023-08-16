package config

import (
	"fmt"
	"log"
	"os"

	//	"github.com/confluentinc/confluent-kafka-go/kafka"
	//	"github.com/rabbitmq/amqp091-go"
	"gopkg.in/yaml.v3"
)

// a type with service configuration parameters
type serviceConfig struct {
	// Port on which the service listens‥
	Port int `json:"port" yaml:"port"`
	// Maximum number of allowed incoming connections.
	MaxConnections int `json:"maxConnections" yaml:"maxConnections"`
}

// global config variables
var Service serviceConfig
var Globus globusConfig
var Databases map[string]databaseConfig
var MessageQueues map[string]messageQueueConfig

// This struct performs the unmarshalling from the YAML config file and then
// copies its fields to the globals above.
type configFile struct {
	Service       serviceConfig                 `yaml:"service"`
	Globus        globusConfig                  `yaml:"globus"`
	Databases     map[string]databaseConfig     `yaml:"databases"`
	MessageQueues map[string]messageQueueConfig `yaml:"message_queues"`
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

	// copy the config data into place
	Service = conf.Service
	Globus = conf.Globus
	Databases = conf.Databases
	MessageQueues = conf.MessageQueues

	return err
}

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

func validateGlobusEndpoints(params globusConfig) error {
	if len(Globus.Endpoints) == 0 {
		return fmt.Errorf("No endpoints were provided!")
	}
	for _, endpoint := range Globus.Endpoints {
		if endpoint.Id.String() == "" { // invalid endpoint UUID
			return fmt.Errorf("Invalid UUID specified for Globus endpoint '%s'", endpoint.Name)
		}
	}
	return nil
}

func validateDatabases(databases map[string]databaseConfig) error {
	if len(databases) == 0 {
		return fmt.Errorf("No databases were provided!")
	}
	for name, db := range databases {
		if db.URL == "" {
			return fmt.Errorf("No URL given for database '%s'", name)
		}
	}
	return nil
}

// This helper validates the given configfile, returning an error that indicates
// success or failure.
func validateConfig() error {
	err := validateServiceParameters(Service)
	if err == nil {
		err = validateGlobusEndpoints(Globus)
		if err == nil {
			err = validateDatabases(Databases)
		}
	}

	return err
}

// Initializes the ID mapping service configuration using the given YAML byte
// data.
func Init(yamlData []byte) error {
	err := readConfig(yamlData)
	if err == nil {
		err = validateConfig()
	}
	return err
}
