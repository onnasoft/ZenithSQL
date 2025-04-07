package storage

// Writer provides data writing operations
type Writer interface {
	Write(values map[string]interface{}) error
	Flush() error
	Close() error
	Commit() error
	Rollback() error
}
