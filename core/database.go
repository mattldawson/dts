package core

// A Database represents an endpoint (source or destination) in a file transfer
// operation database. A database can be queried for available files that can be
// transferred to another database.
type Database struct {
  // the full name of the database
  Name string `yaml:"name"`
	// the name of the organization hosting the database
	Organization string `yaml:"organization"`
	// the base URL at which the database is accessed
	BaseURL string `yaml:"base_url"`
  // a template for a GET request that accepts an ElasticSearch query string
  ESQueryTemplate string `yaml:"es_query_template"`
  // the token in ESQueryTemplate to be replaced by an ElasticSearch query string
  ESQueryToken string `yaml:"es_query_token"`
}
