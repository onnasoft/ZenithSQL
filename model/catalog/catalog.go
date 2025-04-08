package catalog

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
)

type Catalog struct {
	Path      string
	Databases map[string]*Database
	logger    *logrus.Logger
}

type CatalogConfig struct {
	Path   string
	Logger *logrus.Logger
}

func OpenCatalog(config *CatalogConfig) (*Catalog, error) {
	databases := make(map[string]*Database)

	catalogDir, err := os.ReadDir(config.Path)
	if err != nil {
		return nil, fmt.Errorf("error while reading the catalog directory: %v", err)
	}

	for _, dbFS := range catalogDir {
		if dbFS.IsDir() {
			db, err := OpenDatabase(
				&DatabaseConfig{
					Name:   dbFS.Name(),
					Path:   filepath.Join(config.Path, dbFS.Name()),
					Logger: config.Logger,
				},
			)
			if err != nil {
				return nil, fmt.Errorf("error while opening the database %v: %v", dbFS.Name(), err)
			}
			databases[dbFS.Name()] = db
		}
	}

	return &Catalog{
		Path:      config.Path,
		Databases: databases,
		logger:    config.Logger,
	}, nil
}

func (c *Catalog) GetDatabase(name string) (*Database, error) {
	db, ok := c.Databases[name]
	if !ok {
		fmt.Println("Catalog databases:", c.Databases)
		return nil, fmt.Errorf("database %s not found", name)
	}
	return db, nil
}

func (c *Catalog) CreateDatabase(name string) (*Database, error) {
	fullPath := filepath.Join(c.Path, name)
	db, err := NewDatabase(&DatabaseConfig{
		Name:   name,
		Path:   fullPath,
		Logger: c.logger,
	})

	if err != nil {
		return nil, fmt.Errorf("error while creating the database %v: %v", name, err)
	}
	c.Databases[name] = db

	return db, nil
}

func (c *Catalog) Close() error {
	for _, db := range c.Databases {
		if err := db.Close(); err != nil {
			return err
		}
	}
	return nil
}
