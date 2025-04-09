package catalog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/model/types"
	"github.com/sirupsen/logrus"
)

type Schema struct {
	Name          string
	Path          string
	Tables        map[string]*Table
	ConfigManager *storage.ConfigManager
	logger        *logrus.Logger
}

type SchemaConfig struct {
	Name   string
	Path   string
	Logger *logrus.Logger
}

func NewSchema(config *SchemaConfig) (*Schema, error) {
	if err := os.MkdirAll(config.Path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create schema directory: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(config.Path, "tables"), 0755); err != nil {
		return nil, fmt.Errorf("failed to create tables directory: %v", err)
	}
	return OpenSchema(config)
}

func OpenSchema(config *SchemaConfig) (*Schema, error) {
	if _, err := os.Stat(config.Path); err != nil {
		return nil, err
	}

	fullPath := filepath.Join(config.Path, "tables")

	tablesDir, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, fmt.Errorf("error reading %v: %v", tablesDir, err)
	}

	tables := make(map[string]*Table)
	schema := &Schema{
		Name:   config.Name,
		Path:   config.Path,
		logger: config.Logger,
		Tables: tables,
	}

	tablesPath := schema.GetTablesPath()
	configManager := storage.NewConfigManager(tablesPath)
	schema.ConfigManager = configManager

	for _, tableFS := range tablesDir {
		if tableFS.IsDir() {
			_, err := schema.OpenTable(tableFS.Name())
			if err != nil {
				return nil, fmt.Errorf("error opening the table %v: %v", tableFS.Name(), err)
			}
		}
	}

	schema.Tables = tables

	return schema, nil
}

func (s *Schema) GetTablesPath() string {
	return filepath.Join(s.Path, "tables")
}

func (s *Schema) GetTable(name string) (*Table, error) {
	schema, exists := s.Tables[name]
	if !exists {
		return nil, fmt.Errorf("table %s not found", name)
	}
	return schema, nil
}

func (s *Schema) CreateTable(name string, config *storage.TableConfig) (*Table, error) {
	fields := make([]storage.FieldMeta, 1, len(config.Fields)+1)
	fields[0] = storage.FieldMeta{
		Name:     "id",
		Type:     types.Int64,
		Required: true,
		Length:   8,
	}
	fields = append(fields, config.Fields...)
	fields = append(fields, storage.FieldMeta{
		Name:     "created_at",
		Type:     types.Timestamp,
		Required: true,
		Length:   8,
	})
	fields = append(fields, storage.FieldMeta{
		Name:     "updated_at",
		Type:     types.Timestamp,
		Required: true,
		Length:   8,
	})
	fields = append(fields, storage.FieldMeta{
		Name:     "deleted_at",
		Type:     types.Timestamp,
		Required: false,
		Length:   8,
	})

	config.Fields = fields

	if _, err := s.ConfigManager.LoadStats(name); err == nil {
		return nil, fmt.Errorf("table %s already exists", name)
	}
	s.ConfigManager.SaveTableConfig(name, config)

	t, err := OpenTable(&TableConfig{
		Name:          name,
		Path:          filepath.Join(s.Path, "tables"),
		Logger:        s.logger,
		StorageConfig: config,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}
	if err := t.Initialize(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize table: %v", err)
	}

	s.Tables[name] = t
	return t, nil
}

func (s *Schema) OpenTable(name string) (*Table, error) {
	config, err := s.ConfigManager.LoadTableConfig(name)
	if err != nil {
		return nil, fmt.Errorf("failed to load table config: %v", err)
	}

	t, err := OpenTable(&TableConfig{
		Name:          name,
		Path:          filepath.Join(s.Path, "tables"),
		StorageConfig: &config,
		Logger:        s.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}
	s.Tables[name] = t
	return t, nil
}

func (s *Schema) DropTable(name string) error {
	t, err := s.GetTable(name)
	if err != nil {
		return err
	}

	if err := t.Close(); err != nil {
		return fmt.Errorf("failed to close table: %v", err)
	}

	if err := os.RemoveAll(filepath.Join(s.Path, "tables", name)); err != nil {
		return fmt.Errorf("failed to remove table directory: %v", err)
	}

	fmt.Println("Table directory removed:", filepath.Join(s.Path, "tables", name))

	delete(s.Tables, name)

	return nil
}
