package config

type endpointConfig struct {
	// Globus endpoint URLs (if any)
	Globus struct {
		User string `yaml:"user"`
		URL  string `yaml:"url"`
	} `yaml:"globus"`
}
