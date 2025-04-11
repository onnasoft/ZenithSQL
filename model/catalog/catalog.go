package catalog

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/onnasoft/ZenithSQL/core/storage"
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

func (c *Catalog) GetSchema(dbName, schemaName string) (*Schema, error) {
	db, err := c.GetDatabase(dbName)
	if err != nil {
		return nil, err
	}
	schema, err := db.GetSchema(schemaName)
	if err != nil {
		return nil, err
	}
	return schema, nil
}

func (c *Catalog) CreateSchema(dbName, schemaName string) (*Schema, error) {
	db, err := c.GetDatabase(dbName)
	if err != nil {
		return nil, err
	}
	schema, err := db.CreateSchema(schemaName)
	if err != nil {
		return nil, fmt.Errorf("error while creating schema %v: %v", schemaName, err)
	}
	return schema, nil
}

func (c *Catalog) DropSchema(dbName, schemaName string) error {
	db, err := c.GetDatabase(dbName)
	if err != nil {
		return err
	}
	schema, err := db.GetSchema(schemaName)
	if err != nil {
		return err
	}
	schema.Close()

	if err := os.RemoveAll(schema.Path); err != nil {
		return fmt.Errorf("error while removing schema directory %v: %v", schema.Path, err)
	}

	return nil
}

func (c *Catalog) ExistsDatabase(name string) bool {
	_, err := c.GetDatabase(name)
	return err == nil
}

func (c *Catalog) ExistsSchema(dbName, schemaName string) bool {
	_, err := c.GetSchema(dbName, schemaName)
	return err == nil
}

func (c *Catalog) ExistsTable(dbName, schemaName, tableName string) bool {
	_, err := c.GetTable(dbName, schemaName, tableName)
	return err == nil
}

func (c *Catalog) GetTable(dbName, schemaName, tableName string) (*Table, error) {
	db, err := c.GetDatabase(dbName)
	if err != nil {
		return nil, err
	}

	schema, err := db.GetSchema(schemaName)
	if err != nil {
		return nil, err
	}

	table, err := schema.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (c *Catalog) CreateTable(dbName, schemaName string, tableName string, config *storage.TableConfig) (*Table, error) {
	db, err := c.GetDatabase(dbName)
	if err != nil {
		return nil, err
	}

	schema, err := db.GetSchema(schemaName)
	if err != nil {
		return nil, err
	}

	table, err := schema.CreateTable(tableName, config)
	if err != nil {
		return nil, fmt.Errorf("error while creating table %v: %v", tableName, err)
	}

	return table, nil
}

func (c *Catalog) DropTable(dbName, schemaName string, tableName string) error {
	db, err := c.GetDatabase(dbName)
	if err != nil {
		return err
	}

	schema, err := db.GetSchema(schemaName)
	if err != nil {
		return err
	}

	return schema.DropTable(tableName)
}
