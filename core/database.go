package core

type Database struct {
}

func NewDatabase(dbName string) *Database {
	return nil
}

func (db *Database) Search(query string, offset int, maxNum int) (SearchResults, error) {
	results := SearchResults{Files: make([]File, 0)}
	return results, nil
}
