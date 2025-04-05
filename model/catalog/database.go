package catalog

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
)

type Database struct {
	Name    string
	Path    string
	Schemas map[string]*Schema
	logger  *logrus.Logger
}

type DatabaseConfig struct {
	Name   string
	Path   string
	Logger *logrus.Logger
}

func NewDatabase(config *DatabaseConfig) (*Database, error) {
	fullPath := filepath.Join(config.Path, config.Name)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(fullPath, "schemas"), 0755); err != nil {
		return nil, fmt.Errorf("failed to create system directory: %v", err)
	}
	return OpenDatabase(config)
}

func OpenDatabase(config *DatabaseConfig) (*Database, error) {
	fullPath := filepath.Join(config.Path, config.Name, "schemas")

	schemasDir, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}

	schemas := make(map[string]*Schema)
	for _, schemaFS := range schemasDir {
		if schemaFS.IsDir() {
			schema, err := OpenSchema(
				&SchemaConfig{
					Name:   schemaFS.Name(),
					Path:   filepath.Join(fullPath, schemaFS.Name()),
					Logger: config.Logger,
				},
			)
			if err != nil {
				return nil, fmt.Errorf("error while opening the schema %v: %v", schemaFS.Name(), err)
			}

			schemas[schemaFS.Name()] = schema
		}
	}

	return &Database{
		Name:    config.Name,
		Path:    fullPath,
		Schemas: schemas,
		logger:  config.Logger,
	}, nil
}

func (db *Database) CreateSchema(name string) (*Schema, error) {
	schema, err := NewSchema(&SchemaConfig{
		Name:   name,
		Path:   filepath.Join(db.Path, "schemas", name),
		Logger: db.logger,
	})
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
