package engine

import (
	"fmt"
	"os"
	"path/filepath"

	logrus "github.com/sirupsen/logrus"
)

var log = logrus.New()

type Database struct {
	Name    string
	Path    string
	Schemas map[string]*Schema
}

func NewDatabase(name, path string) (*Database, error) {
	fullPath := filepath.Join(path, name)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(fullPath, "schemas"), 0755); err != nil {
		return nil, fmt.Errorf("failed to create system directory: %v", err)
	}
	return &Database{
		Name:    name,
		Path:    fullPath,
		Schemas: make(map[string]*Schema),
	}, nil
}

func OpenDatabase(name, path string) (*Database, error) {
	fullPath := filepath.Join(path, name, "schemas")

	schemasDir, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}

	schemas := make(map[string]*Schema)
	for _, schemaFS := range schemasDir {
		if schemaFS.IsDir() {
			schema, err := OpenSchema(
				schemaFS.Name(),
				filepath.Join(fullPath, schemaFS.Name()),
			)
			if err != nil {
				return nil, fmt.Errorf("error while opening the schema %v: %v", schemaFS.Name(), err)
			}

			schemas[schemaFS.Name()] = schema
		}
	}

	return &Database{
		Name:    name,
		Path:    fullPath,
		Schemas: schemas,
	}, nil
}

func (db *Database) CreateSchema(name string) (*Schema, error) {
	schema, err := NewSchema(name, filepath.Join(db.Path, "schemas", name))
	if err != nil {
		return nil, fmt.Errorf("failed to create schema: %v", err)
	}
	db.Schemas[name] = schema
	return schema, nil
}

func (db *Database) GetSchema(name string) (*Schema, error) {
	schema, exists := db.Schemas[name]
	if !exists {
		return nil, fmt.Errorf("schema %s not found", name)
	}
	return schema, nil
}

func (db *Database) Close() error {
	return nil
}
