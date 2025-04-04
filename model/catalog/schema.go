package catalog

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/onnasoft/ZenithSQL/model/entity"
)

type Schema struct {
	Name   string
	Path   string
	Tables map[string]*Table
}

func NewSchema(name, path string) (*Schema, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create schema directory: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(path, "tables"), 0755); err != nil {
		return nil, fmt.Errorf("failed to create tables directory: %v", err)
	}
	return &Schema{
		Name:   name,
		Path:   path,
		Tables: make(map[string]*Table),
	}, nil
}

func OpenSchema(name, path string) (*Schema, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}

	fullPath := filepath.Join(path, "tables")

	tablesDir, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, fmt.Errorf("error reading %v: %v", tablesDir, err)
	}

	tables := make(map[string]*Table)
	for _, tableFS := range tablesDir {
		if tableFS.IsDir() {
			table, err := OpenTable(&TableConfig{
				Name: tableFS.Name(),
				Path: fullPath,
			})

			if err != nil {
				return nil, fmt.Errorf("error opening the table %v: %v", tableFS.Name(), err)
			}

			tables[tableFS.Name()] = table
		}
	}

	return &Schema{
		Name:   name,
		Path:   path,
		Tables: tables,
	}, nil
}

func (s *Schema) GetTable(name string) (*Table, error) {
	schema, exists := s.Tables[name]
	if !exists {
		return nil, fmt.Errorf("table %s not found", name)
	}
	return schema, nil
}

func (s *Schema) CreateTable(name string, fields []*entity.Field) (*Table, error) {
	t, err := NewTable(&TableConfig{
		Name:   name,
		Path:   filepath.Join(s.Path, "tables"),
		Fields: fields,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}
	s.Tables[name] = t
	return t, nil
}

func (s *Schema) OpenTable(name string) (*Table, error) {
	t, err := OpenTable(&TableConfig{
		Name: name,
		Path: filepath.Join(s.Path, "tables"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}
	s.Tables[name] = t
	return t, nil
}
