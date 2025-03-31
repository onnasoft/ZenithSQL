package dataframe

type Schema struct {
	Name   string
	Tables map[string]*Table
}

func NewSchema(name string) *Schema {
	return &Schema{Name: name, Tables: make(map[string]*Table)}
}

func (db *Database) CreateSchema(name string) *Schema {
	schema := NewSchema(name)
	db.Schemas[name] = schema
	return schema
}
