package columnstorage

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/model/fields"
	"github.com/sirupsen/logrus"
)

const statsFileName = "stats.bin"

type ColumnStorage struct {
	fields        fields.FieldsMeta
	columns       map[string]*Column
	BasePath      string
	StatsFilePath string
	Logger        *logrus.Logger
	StorageStats  *storage.StorageStats

	insertLock sync.Mutex
	importLock sync.Mutex
}

type ColumnStorageConfig struct {
	Fields        fields.FieldsMeta
	BasePath      string
	StatsFilePath string
	StorageStats  *storage.StorageStats
	Logger        *logrus.Logger
}

func NewColumnStorage(cfg *ColumnStorageConfig) storage.Storage {
	store := &ColumnStorage{
		fields:        cfg.Fields,
		BasePath:      cfg.BasePath,
		StatsFilePath: cfg.StatsFilePath,
		StorageStats:  cfg.StorageStats,
		Logger:        cfg.Logger,
	}

	if cfg.StatsFilePath == "" {
		store.StatsFilePath = cfg.BasePath + "/" + statsFileName
	}

	return store
}

func (s *ColumnStorage) Initialize(ctx context.Context) error {
	columns := make(map[string]*Column)

	for i := 0; i < len(s.fields); i++ {
		meta := s.fields[i]
		dataType := fields.NewDataType(meta.Type)
		col, err := NewColumn(meta.Name, dataType, meta.Length, meta.Required, s.BasePath)
		if err != nil {
			return fmt.Errorf("failed to initialize column %s: %w", meta.Name, err)
		}
		if col.MMapFile == nil {
			return fmt.Errorf("column %s has nil MMapFile", meta.Name)
		}
		columns[meta.Name] = col
	}

	s.columns = columns

	return nil
}

func (s *ColumnStorage) Truncate() error {
	s.Lock()
	defer s.Unlock()

	for _, col := range s.columns {
		if err := col.Truncate(); err != nil {
			s.Logger.Error("Failed to truncate column ", col.Name(), err)
			return err
		}
	}
	s.StorageStats.TotalRows = 0
	s.StorageStats.SaveToFile(s.StatsFilePath)
	return nil
}

func (s *ColumnStorage) Close() error {
	for _, col := range s.columns {
		if err := col.Close(); err != nil {
			s.Logger.WithError(err).Error("Failed to close column")
		}
	}
	return nil
}

func (s *ColumnStorage) Backup(ctx context.Context, writer io.Writer) error {
	return nil
}

func (s *ColumnStorage) Restore(ctx context.Context, reader io.Reader) error {
	return nil
}

func (s *ColumnStorage) Stats() storage.StorageStats {
	return *s.StorageStats
}

func (s *ColumnStorage) Compact(ctx context.Context) error {
	return nil
}

func (s *ColumnStorage) CreateField(meta fields.FieldMeta, validators ...storage.Validator) error {
	return nil
}

func (s *ColumnStorage) DeleteField(name string) error {
	return nil
}

func (s *ColumnStorage) GetFieldMeta(name string) (fields.FieldMeta, error) {
	return fields.FieldMeta{}, nil
}

func (s *ColumnStorage) ListFields() ([]fields.FieldMeta, error) {
	return nil, nil
}

func (s *ColumnStorage) UpdateField(name string, newMeta fields.FieldMeta) error {
	return nil
}

func (s *ColumnStorage) Writer() (storage.Writer, error) {
	return NewColumnWriter(s.columns), nil
}

func (s *ColumnStorage) Reader() (storage.Reader, error) {
	return NewColumnReader(s.columns, s.StorageStats)
}

func (s *ColumnStorage) Cursor() (storage.Cursor, error) {
	reader, err := NewColumnReader(s.columns, s.StorageStats)
	if err != nil {
		return nil, err
	}
	return NewColumnCursor(reader), nil
}

func (s *ColumnStorage) Lock() error {
	s.LockImport()
	return nil
}

func (s *ColumnStorage) Unlock() error {
	s.UnlockImport()
	return nil
}

func (s *ColumnStorage) LockInsert() error {
	s.insertLock.Lock()
	return nil
}

func (s *ColumnStorage) UnlockInsert() error {
	s.insertLock.Unlock()
	return nil
}

func (s *ColumnStorage) LockImport() error {
	s.importLock.Lock()
	return nil
}

func (s *ColumnStorage) UnlockImport() error {
	s.importLock.Unlock()
	return nil
}

func (t *ColumnStorage) GetNextID() int64 {
	return t.StorageStats.TotalRows + 1
}

func (t *ColumnStorage) RowCount() int64 {
	return t.StorageStats.TotalRows
}

func (t *ColumnStorage) UpdateRowCount(count int64) error {
	atomic.StoreInt64(&t.StorageStats.TotalRows, count)
	t.StorageStats.LastModified = time.Now()

	return t.StorageStats.SaveToFile(t.StatsFilePath)
}

func (t *ColumnStorage) Columns() map[string]*Column {
	return t.columns
}
