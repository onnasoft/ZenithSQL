package columnstorage

import (
	"context"
	"io"
	"sync"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/sirupsen/logrus"
)

type ColumnStorage struct {
	fields       []storage.FieldMeta
	columns      map[string]*Column
	BasePath     string
	Logger       *logrus.Logger
	StorageStats *storage.StorageStats

	insertLock *sync.Mutex
	importLock *sync.Mutex
}

type ColumnStorageConfig struct {
	BasePath     string
	Fields       []storage.FieldMeta
	StorageStats *storage.StorageStats
	Logger       *logrus.Logger
}

func NewColumnStorage(cfg *ColumnStorageConfig) *ColumnStorage {
	store := &ColumnStorage{
		fields:       cfg.Fields,
		BasePath:     cfg.BasePath,
		StorageStats: cfg.StorageStats,
		Logger:       cfg.Logger,
	}

	return store
}

func (s *ColumnStorage) Initialize(ctx context.Context) error {
	columns := make(map[string]*Column)

	for _, meta := range s.fields {
		col, err := NewColumn(meta.Name, meta.Type, meta.Length, meta.Required, s.BasePath)
		if err != nil {
			s.Logger.WithError(err).Errorf("Failed to create column %s", meta.Name)
			continue
		}
		columns[meta.Name] = col
	}

	s.columns = columns

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
	return storage.StorageStats{}
}

func (s *ColumnStorage) Compact(ctx context.Context) error {
	return nil
}

func (s *ColumnStorage) CreateField(ctx context.Context, meta storage.FieldMeta, validators ...storage.Validator) error {
	return nil
}

func (s *ColumnStorage) DeleteField(ctx context.Context, name string) error {
	return nil
}

func (s *ColumnStorage) GetFieldMeta(ctx context.Context, name string) (storage.FieldMeta, error) {
	return storage.FieldMeta{}, nil
}

func (s *ColumnStorage) ListFields(ctx context.Context) ([]storage.FieldMeta, error) {
	return nil, nil
}

func (s *ColumnStorage) UpdateField(ctx context.Context, name string, newMeta storage.FieldMeta) error {
	return nil
}

func (s *ColumnStorage) Writer(ctx context.Context) (storage.Writer, error) {
	return nil, nil
}

func (s *ColumnStorage) Reader(ctx context.Context) (storage.Reader, error) {
	return nil, nil
}

func (s *ColumnStorage) BulkInsert(ctx context.Context, values []map[string]interface{}) error {
	return nil
}

func (s *ColumnStorage) Delete(ctx context.Context, filter storage.Filter) (int64, error) {
	return 0, nil
}

func (s *ColumnStorage) Aggregate(ctx context.Context, field string, aggFunc storage.AggregationFunc, filter storage.Filter) (interface{}, error) {
	return nil, nil
}

func (s *ColumnStorage) Search(ctx context.Context, filter storage.Filter, fields ...string) (storage.Cursor, error) {
	return nil, nil
}

func (s *ColumnStorage) Distinct(ctx context.Context, field string, filter storage.Filter) ([]interface{}, error) {
	return nil, nil
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
	s.insertLock.Lock()
	return nil
}

func (s *ColumnStorage) UnlockImport() error {
	s.importLock.Unlock()
	s.insertLock.Unlock()
	return nil
}
