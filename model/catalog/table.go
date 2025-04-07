package catalog

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/onnasoft/ZenithSQL/core/providers/columnstorage"
	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/sirupsen/logrus"
)

type Table struct {
	Name          string
	Path          string
	Logger        *logrus.Logger
	StorageConfig *storage.TableConfig
	storage.Storage
}

type TableConfig struct {
	Name          string
	Path          string
	Logger        *logrus.Logger
	StorageConfig *storage.TableConfig
}

func OpenTable(config *TableConfig) (*Table, error) {
	storage := columnstorage.NewColumnStorage(&columnstorage.ColumnStorageConfig{
		BasePath:     filepath.Join(config.Path, config.Name),
		Fields:       config.StorageConfig.Fields,
		StorageStats: &config.StorageConfig.Stats,
		Logger:       config.Logger,
	})

	if err := storage.Initialize(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	table := &Table{
		Name:          config.Name,
		Path:          config.Path,
		Logger:        config.Logger,
		StorageConfig: config.StorageConfig,
		Storage:       storage,
	}

	if config.StorageConfig == nil {
		return nil, fmt.Errorf("storage config is nil")
	}

	return table, nil
}
