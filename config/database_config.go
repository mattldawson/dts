package config

// A database provides files for a file transfer (at its source or destination).
type databaseConfig struct {
	// the full name of the database
	Name string `yaml:"name"`
	// the name of the organization hosting the database
	Organization string `yaml:"organization"`
	// the base URL at which the database is accessed
	URL string `yaml:"url"`
	// the name of an endpoint for this database
	Endpoint string `yaml:"endpoint"`
	// authorization data (client secret passed in headers to authorize requests)
	Auth authConfig `yaml:"auth"`
	// the name of a monitoring service used by this database for notifications
	Notifications string `yaml:"notifications"`
	// search instructions
	Search struct {
		// ElasticSearch parameters
		ElasticSearch struct {
			// the resource for ElasticSearch queries (GET)
			Resource string `yaml:"resource"`
			// the parameter to which an ES query string is passed
			QueryParameter string `yaml:"query_parameter"`
		} `yaml:"elasticsearch"`
	} `yaml:"search"`
	// transfer initiation instructions
	Initiate struct {
		// transfer initiation resource (POST)
		Resource string `yaml:"resource"`
		// transfer initiation request body fields
		Request map[string]string `yaml:"request"`
	} `yaml:"initiate"`
	// file inspection instructions
	Inspect struct {
		// file inspection resource (POST)
		Resource string `yaml:"resource"`
		// file inspection request body fields
		Request map[string]string `yaml:"request"`
	} `yaml:"inspect"`
}
