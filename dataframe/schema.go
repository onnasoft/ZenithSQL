package dataframe

import (
	"fmt"
	"os"
	"path/filepath"
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

func (s *Schema) CreateTable(name string) (*Table, error) {
	t, err := NewTable(name, filepath.Join(s.Path, "tables"))
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}
	s.Tables[name] = t
	return t, nil
}
