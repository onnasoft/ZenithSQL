package columnstorage

import (
	"fmt"
	"path/filepath"

	"github.com/onnasoft/ZenithSQL/core/buffer"
	"github.com/onnasoft/ZenithSQL/model/types"
	"github.com/onnasoft/ZenithSQL/validate"
)

type Column struct {
	Name       string
	DataType   types.DataType
	Length     int
	Required   bool
	Validators []validate.Validator
	isValid    func(val interface{}) error
	write      func(buffer []byte, val interface{}) error

	BasePath string
	MMapFile *buffer.MMapFile
}

func NewColumn(name string, dataType types.DataType, length int, required bool, basePath string) (*Column, error) {
	write, err := dataType.Writer()
	if err != nil {
		return nil, fmt.Errorf("failed to get writer for data type %s: %w", dataType, err)
	}

	col := &Column{
		Name:     name,
		DataType: dataType,
		Length:   length,
		Required: required,
		BasePath: basePath,
		isValid:  dataType.Valid(),
		write:    write,
	}

	if err := col.init(); err != nil {
		return nil, err
	}

	return col, nil
}

func (c *Column) init() error {
	filepath := filepath.Join(c.BasePath, c.Name+".data")
	buff, err := buffer.Open(filepath, 0)
	if err != nil {
		return err
	}
	c.MMapFile = buff

	fmt.Println("Column buffer initialized:", filepath)

	return nil
}

func (c *Column) Close() error {
	if err := c.MMapFile.Close(); err != nil {
		return fmt.Errorf("failed to close buffer: %w", err)
	}
	return nil
}
