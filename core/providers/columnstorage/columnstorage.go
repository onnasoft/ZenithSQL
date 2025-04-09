package columnstorage

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/model/types"
	"github.com/sirupsen/logrus"
)

const statsFileName = "stats.bin"

type ColumnStorage struct {
	fields        []storage.FieldMeta
	columns       map[string]*Column
	BasePath      string
	StatsFilePath string
	Logger        *logrus.Logger
	StorageStats  *storage.StorageStats

	insertLock sync.Mutex
	importLock sync.Mutex
}

type ColumnStorageConfig struct {
	BasePath      string
	StatsFilePath string
	Fields        []storage.FieldMeta
	StorageStats  *storage.StorageStats
	Logger        *logrus.Logger
}

func NewColumnStorage(cfg *ColumnStorageConfig) *ColumnStorage {
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
		dataType := types.NewDataType(meta.Type)
		col, err := NewColumn(meta.Name, dataType, meta.Length, meta.Required, s.BasePath)
		if err != nil {
			s.Logger.Error("Failed to create column ", err)
			continue
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
			s.Logger.WithError(err).Errorf("Failed to truncate column %s", col.Name)
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

func (s *ColumnStorage) CreateField(meta storage.FieldMeta, validators ...storage.Validator) error {
	return nil
}

func (s *ColumnStorage) DeleteField(name string) error {
	return nil
}

func (s *ColumnStorage) GetFieldMeta(name string) (storage.FieldMeta, error) {
	return storage.FieldMeta{}, nil
}

func (s *ColumnStorage) ListFields() ([]storage.FieldMeta, error) {
	return nil, nil
}

func (s *ColumnStorage) UpdateField(name string, newMeta storage.FieldMeta) error {
	return nil
}

func (s *ColumnStorage) Writer() (storage.Writer, error) {
	return NewColumnWriter(s.columns), nil
}

func (s *ColumnStorage) Reader() (storage.Reader, error) {
	return NewColumnReader(s.columns, s.StorageStats), nil
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
