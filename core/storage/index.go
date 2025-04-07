package storage

// Index provides indexing capabilities
type Index interface {
	Add(value interface{}, position int64) error
	Remove(value interface{}, position int64) error
	Find(filter Filter) ([]int64, error)
	Rebuild() error
	Stats() IndexStats
}

// IndexStats contains index statistics
type IndexStats struct {
	Size         int64
	UniqueValues int64
	MemoryUsage  int64
}
