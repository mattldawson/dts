package databases

// This type represents credit-related metadata associated with a DataResource.
// It's based on the schema for KBase's CreditMetadata type
// (https://github.com/kbase/credit_engine/blob/develop/schema/kbase/linkml/metadata.yaml)
type CreditMetadata struct {
	// list of strings of freeform text providing extra information about this
	// credit metadata
	Comment []string `json:"comment,omitempty"`
	// a brief description or abstract for the resource being represented
	Description string `json:"description,omitempty"`
	// resolvable persistent unique identifier for the resource. Should be in the
	// format <database name>:<identifier within database>.
	Identifier string `json:"identifier"`
	// Usage license for the resource. May be a string or an URL.
	License string `json:"license,omitempty"`
}
