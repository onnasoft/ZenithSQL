package catalog

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/onnasoft/ZenithSQL/model/entity"
	"github.com/sirupsen/logrus"
)

type Schema struct {
	Name   string
	Path   string
	Tables map[string]*Table
	logger *logrus.Logger
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
	for _, tableFS := range tablesDir {
		if tableFS.IsDir() {
			table, err := OpenTable(&TableConfig{
				Name:   tableFS.Name(),
				Path:   fullPath,
				Logger: config.Logger,
			})

			if err != nil {
				return nil, fmt.Errorf("error opening the table %v: %v", tableFS.Name(), err)
			}

			tables[tableFS.Name()] = table
		}
	}

	return &Schema{
		Name:   config.Name,
		Path:   config.Path,
		Tables: tables,
		logger: config.Logger,
	}, nil
}

func (s *Schema) GetTable(name string) (*Table, error) {
	schema, exists := s.Tables[name]
	if !exists {
		return nil, fmt.Errorf("table %s not found", name)
	}
	return schema, nil
}

func (s *Schema) CreateTable(name string, schema *entity.Schema) (*Table, error) {
	t, err := NewTable(&TableConfig{
		Name:   name,
		Path:   filepath.Join(s.Path, "tables"),
		Schema: schema,
		Logger: s.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}
	s.Tables[name] = t
	return t, nil
}

func (s *Schema) OpenTable(name string) (*Table, error) {
	t, err := OpenTable(&TableConfig{
		Name:   name,
		Path:   filepath.Join(s.Path, "tables"),
		Logger: s.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}
	s.Tables[name] = t
	return t, nil
}
