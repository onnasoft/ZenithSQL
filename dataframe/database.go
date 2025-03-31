package dataframe

type Database struct {
	Name    string
	Schemas map[string]*Schema
}

func NewDatabase(name string) *Database {
	return &Database{
		Name:    name,
		Schemas: make(map[string]*Schema),
	}
}
