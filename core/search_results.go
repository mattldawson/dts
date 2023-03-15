package core

// this type stores the results from an ElasticSearch query
// FIXME: for now, we use the structure defined by the JGI Data Portal, but
// FIXME: as we expand our scope this will likely change to a more generic
// FIXME: ElasticSearch setup (e.g. https://github.com/elastic/go-elasticsearch)
type SearchResults struct {
	Files []File `json:"files"`
}
