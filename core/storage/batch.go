package storage

// BatchProcessor provides batch processing
type BatchProcessor interface {
	ProcessBatch(batch []map[string]interface{}) error
	Flush() error
}
