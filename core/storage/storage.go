package storage

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"sync/atomic"
	"time"
	"unsafe"
)

type Validator interface {
	Validate(value interface{}) error
}

type storageStats struct {
	TotalRows    int64
	LastModified int64
}

type StorageStats struct {
	TotalRows    int64
	LastModified time.Time
}

func (s *StorageStats) UpdateTotalRows(count int64) {
	s.TotalRows += count
}

func (s *StorageStats) SaveToFile(filePath string) error {
	s.LastModified = time.Now()
	temp := storageStats{
		TotalRows:    s.TotalRows,
		LastModified: s.LastModified.Unix(),
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := binary.Write(file, binary.LittleEndian, temp); err != nil {
		return err
	}

	return nil
}

func (s *StorageStats) LoadFromFile(filePath string) error {
	temp := storageStats{}

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := binary.Read(file, binary.LittleEndian, &temp); err != nil {
		return err
	}

	atomic.StoreInt64(&s.TotalRows, temp.TotalRows)
	lastModified := time.Unix(temp.LastModified, 0)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&s.LastModified)), unsafe.Pointer(&lastModified))

	return nil
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

	Truncate() error

	Writer() (Writer, error)
	Reader() (Reader, error)
	Cursor() (Cursor, error)

	Lock() error
	Unlock() error
	LockInsert() error
	UnlockInsert() error
	LockImport() error
	UnlockImport() error

	GetNextID() int64
	RowCount() int64
	UpdateRowCount(count int64) error
}
