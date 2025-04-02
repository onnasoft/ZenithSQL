package engine

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/onnasoft/ZenithSQL/entity"
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

func (s *Schema) LoadTable(name string) (*Table, error) {
	t, err := LoadTable(&TableConfig{
		Name: name,
		Path: filepath.Join(s.Path, "tables"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}
	s.Tables[name] = t
	return t, nil
}
