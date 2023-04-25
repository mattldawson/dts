package databases

import (
	"dts/databases/credit"
)

// a Frictionless data resource describing a file in a search
// (https://specs.frictionlessdata.io/data-resource/)
type DataResource struct {
	// a name (CURIE ID) used to uniquely identify a resource in different databases
	// (format is SOURCE:ID, e.g. JDP:<ITS project ID>)
	Name string `json:"name"`
	// a relative path to the resource's file within a data package directory
	Path string `json:"path"`
	// a title or label for the resource (optional)
	Title string `json:"title,omitempty"`
	// a description of the resource (optional)
	Description string `json:"description,omitempty"`
	// indicates the format of the resource's file, often used as an extension
	Format string `json:"format"`
	// the mediatype/mimetype of the resource (e.g. "test/csv")
	MediaType string `json:"media_type,omitempty"`
	// the character encoding for the resource's file (default: UTF-8)
	Encoding string `json:"encoding,omitempty"`
	// the size of the resource's file in bytes
	Bytes int `json:"bytes"`
	// the MD5 hash for the resource's file (other algorithms are indicated with
	// a prefix to the hash delimited by a colon)
	Hash string `json:"hash"`
	// a list identifying the sources for this resource
	Sources []struct {
		// a descriptive title for the source
		Title string `json:"title"`
		// a URI or relative path pointing to the source (optional)
		Path string `json:"path,omitempty"`
		// an email address identifying a contact associated with the source (optional)
		Email string `json:"email,omitempty"`
	} `json:"sources,omitempty"`
	// a list identifying the license or licenses under which this resource is
	// managed
	Licenses []struct {
		// the abbreviated name of the license
		Name string `json:"name"`
		// a URI or relative path at which the license text may be retrieved
		Path string `json:"path"`
		// the descriptive title of the license (optional)
		Title string `json:"title,omitempty"`
	} `json:"licenses,omitempty"`
	// credit metadata associated with the resource (optional for now)
	Credit credit.CreditMetadata `json:"credit,omitempty"`
}
