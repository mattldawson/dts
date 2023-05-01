package databases

import (
	"fmt"

	"dts/core"
	"dts/databases/jdp"
)

// creates a database based on the configured type
func NewDatabase(dbName string) (core.Database, error) {
	if dbName == "jdp" {
		return jdp.NewDatabase(dbName)
	} else {
		return nil, fmt.Errorf("Unknown database type for '%s'", dbName)
	}
}
