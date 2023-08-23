package databases

import (
	"fmt"

	"dts/core"
	"dts/databases/jdp"
)

// we maintain a table of database instances, identified by their names
var allDatabases map[string]core.Database = make(map[string]core.Database)

// creates a database based on the configured type, or returns an existing
// instance
func NewDatabase(dbName string) (core.Database, error) {
	var err error

	// do we have one of these already?
	db, found := allDatabases[dbName]
	if !found {
		// go get one
		if dbName == "jdp" {
			db, err = jdp.NewDatabase(dbName)
		} else {
			err = fmt.Errorf("Unknown database type for '%s'", dbName)
		}
		if err == nil {
			allDatabases[dbName] = db // stash it
		}
	}
	return db, err
}
