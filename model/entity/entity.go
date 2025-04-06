package entity

import "github.com/onnasoft/ZenithSQL/core/buffer"

type Entity interface {
	SetValue(name string, value interface{}) error
	GetValue(name string) interface{}
	IsSetted() bool
	Save() error
	Reset()
	Values() map[string]interface{}
	Len() int
	String() string
	RW() buffer.ReadWriter
	Schema() *Schema
}

type EntityConfig struct {
	Schema *Schema
	RW     buffer.ReadWriter
	Cache  bool
}

func NewEntity(config *EntityConfig) (Entity, error) {
	if config.Cache {
		return newCachedEntity(config)
	}
	return newStatelessEntity(config)
}
