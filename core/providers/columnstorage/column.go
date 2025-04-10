package columnstorage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/onnasoft/ZenithSQL/core/buffer"
	"github.com/onnasoft/ZenithSQL/model/types"
	"github.com/onnasoft/ZenithSQL/validate"
)

type ColumnData struct {
	*Column
	data []byte
}

func (c *ColumnData) Name() string {
	return c.Column.name
}

type Column struct {
	name string
	types.DataType
	Length     int
	Required   bool
	Validators []validate.Validator

	BasePath string
	MMapFile *buffer.MMapFile
}

func (c *Column) Type() types.DataType {
	return c.DataType
}

func (c *Column) String() string {
	return fmt.Sprintf("Name: %s, Type: %s, Length: %d, Required: %t", c.Name(), c.DataType.String(), c.Length, c.Required)
}

func (c *Column) Name() string {
	return c.name
}

func NewColumn(name string, dataType types.DataType, length int, required bool, basePath string) (*Column, error) {
	effectiveLength, err := dataType.ResolveLength(length)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve length for data type %s: %w", dataType, err)
	}

	col := &Column{
		name:     name,
		DataType: dataType,
		Length:   effectiveLength,
		Required: required,
		BasePath: basePath,
	}

	if err := col.init(); err != nil {
		return nil, err
	}

	return col, nil
}

func (c *Column) init() error {
	filepath := filepath.Join(c.BasePath, c.name+".data")
	buff, err := buffer.Open(filepath, 0, (c.Length+2)*10_000_000)
	if err != nil {
		return err
	}
	c.MMapFile = buff

	return nil
}

func (c *Column) Truncate() error {
	path := filepath.Join(c.BasePath, c.name+".data")

	if _, err := os.Stat(path); err == nil {
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("failed to remove file %s: %w", path, err)
		}
		log.Printf("Removed existing file %s", path)
	}

	return c.init()
}

func (c *Column) Close() error {
	return nil
}
