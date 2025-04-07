package storage

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/onnasoft/ZenithSQL/model/types"
)

type FieldMeta struct {
	Name       string          `json:"name"`
	Type       types.DataType  `json:"type"`
	Length     int             `json:"length"`
	Required   bool            `json:"required,omitempty"`
	Validators []ValidatorInfo `json:"validators,omitempty"`
}

type ValidatorInfo struct {
	Type   string          `json:"type"`
	Params json.RawMessage `json:"params"`
}

type Validator interface {
	Validate(value interface{}) error
}

type storageStats struct {
	TotalRows    int64
	TotalSize    int64
	LastModified int64
}

type StorageStats struct {
	TotalRows    int64
	TotalSize    int64
	LastModified time.Time
}

// FieldStats contains column-specific statistics
type FieldStats struct {
	DiskSize      int64       `json:"disk_size"`
	NullCount     int64       `json:"null_count"`
	DistinctCount int64       `json:"distinct_count"`
	MinValue      interface{} `json:"min_value,omitempty"`
	MaxValue      interface{} `json:"max_value,omitempty"`
}

// StorageConfig contains storage configuration
type StorageConfig struct {
	BasePath   string `json:"base_path"`
	BufferSize int    `json:"buffer_size"`
}

// Storage is the main storage interface
type Storage interface {
	Initialize(ctx context.Context) error
	Close() error
	Backup(ctx context.Context, writer io.Writer) error
	Restore(ctx context.Context, reader io.Reader) error
	Stats() StorageStats
	Compact(ctx context.Context) error

	CreateField(ctx context.Context, meta FieldMeta, validators ...Validator) error
	DeleteField(ctx context.Context, name string) error
	GetFieldMeta(ctx context.Context, name string) (FieldMeta, error)
	ListFields(ctx context.Context) ([]FieldMeta, error)
	UpdateField(ctx context.Context, name string, newMeta FieldMeta) error

	Writer(ctx context.Context) (Writer, error)
	Reader(ctx context.Context) (Reader, error)
	BulkInsert(ctx context.Context, values []map[string]interface{}) error
	Delete(ctx context.Context, filter Filter) (int64, error)

	Aggregate(ctx context.Context, field string, aggFunc AggregationFunc, filter Filter) (interface{}, error)
	Search(ctx context.Context, filter Filter, fields ...string) (Cursor, error)
	Distinct(ctx context.Context, field string, filter Filter) ([]interface{}, error)

	LockInsert() error
	UnlockInsert() error

	GetNextID() int64
	RowCount() int64
	UpdateRowCount(count int64) error
}
