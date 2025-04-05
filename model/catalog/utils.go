package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/onnasoft/ZenithSQL/model/entity"
)

func saveSchema(filePath string, schema *entity.Schema) error {
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")

	fields := make([]interface{}, schema.Len())
	for i := 0; i < schema.Len(); i++ {
		field, _ := schema.GetField(i)
		fields[i] = field.ToMap()
	}

	return enc.Encode(map[string]interface{}{
		"fields": fields,
	})
}

func loadSchema(filePath string) (*entity.Schema, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var rawData map[string]interface{}
	if err := json.NewDecoder(file).Decode(&rawData); err != nil {
		return nil, err
	}

	var rawFields []interface{} = rawData["fields"].([]interface{})

	fields := make([]*entity.Field, len(rawFields))
	for i, fieldMap := range rawFields {
		field := &entity.Field{}
		if err := field.FromMap(fieldMap.(map[string]interface{})); err != nil {
			return nil, err
		}
		fields[i] = field
	}

	schema := entity.NewSchema()
	for _, field := range fields {
		if err := schema.AddField(field); err != nil {
			return nil, err
		}
	}

	return schema, nil
}

func makeMeta() *entity.Schema {
	meta := entity.NewSchema()
	meta.AddField(NewFieldUInt64("id"))
	meta.AddField(NewFieldTimestamp("created_at"))
	meta.AddField(NewFieldTimestamp("updated_at"))
	meta.AddField(NewFieldTimestamp("deleted_at"))

	return meta
}

func validateConfig(config *TableConfig) error {
	if config == nil {
		return errors.New("config cannot be nil")
	}
	if config.Name == "" {
		return errors.New("name cannot be empty")
	}
	if config.Path == "" {
		return errors.New("path cannot be empty")
	}
	if config.Logger == nil {
		return errors.New("logger cannot be nil")
	}
	return nil
}

func ensureTableDirectoryExists(base string) error {
	if _, err := os.Stat(base); os.IsNotExist(err) {
		return fmt.Errorf("table directory does not exist: %s", base)
	}
	return nil
}

func loadOrCreateMeta(metaFile string) (*entity.Schema, error) {
	if _, err := os.Stat(metaFile); os.IsNotExist(err) {
		meta := makeMeta()
		meta.Lock()
		if err := saveSchema(metaFile, meta); err != nil {
			return nil, fmt.Errorf("failed to save schema: %w", err)
		}
		return meta, nil
	}

	schema, err := loadSchema(metaFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load schema: %w", err)
	}
	schema.Lock()
	return schema, nil
}

func loadOrLockSchema(config *TableConfig, schemaFile string) error {
	if config.Schema == nil {
		schema, err := loadSchema(schemaFile)
		if err != nil {
			return err
		}
		config.Schema = schema
		config.Schema.Lock()
	}

	if !config.Schema.IsLocked() {
		return errors.New("schema is not locked")
	}
	return nil
}
