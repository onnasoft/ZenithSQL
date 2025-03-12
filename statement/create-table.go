package statement

import (
	"github.com/onnasoft/sql-parser/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CreateTableStatement struct {
	TableName string             `msgpack:"table_name" json:"table_name"`
	Columns   []ColumnDefinition `msgpack:"columns" json:"columns"`
	Storage   string             `msgpack:"storage" json:"storage"`
}

type ColumnDefinition struct {
	Name         string `msgpack:"name" json:"name"`
	Type         string `msgpack:"type" json:"type"`
	Length       int    `msgpack:"length" json:"length"`
	PrimaryKey   bool   `msgpack:"primary_key" json:"primary_key"`
	Index        bool   `msgpack:"index" json:"index"`
	DefaultValue string `msgpack:"default_value" json:"default_value"`
}

func (c *CreateTableStatement) Protocol() protocol.MessageType {
	return protocol.CreateTable
}

func (c *CreateTableStatement) Serialize() ([]byte, error) {
	msgpackBytes, err := msgpack.Marshal(c)
	if err != nil {
		return nil, err
	}

	length := len(msgpackBytes)
	prefixedBytes := make([]byte, 4+length)
	prefixedBytes[0] = byte(length >> 24)
	prefixedBytes[1] = byte(length >> 16)
	prefixedBytes[2] = byte(length >> 8)
	prefixedBytes[3] = byte(length)

	copy(prefixedBytes[4:], msgpackBytes)

	return prefixedBytes, nil
}

func (c *CreateTableStatement) Deserialize(data []byte) error {
	if len(data) < 4 {
		return NewInvalidMessagePackDataError()
	}

	length := int(data[0])<<24 | int(data[1])<<16 | int(data[2])<<8 | int(data[3])

	if len(data[4:]) != length {
		return NewInvalidMessagePackDataError()
	}

	err := msgpack.Unmarshal(data[4:], c)
	if err != nil {
		return err
	}

	return nil
}
