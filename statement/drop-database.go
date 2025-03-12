package statement

import (
	"github.com/onnasoft/sql-parser/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type DropDatabaseStatement struct {
	DatabaseName string `msgpack:"database_name" json:"database_name" valid:"required,alphanumunderscore"`
}

func (d *DropDatabaseStatement) Protocol() protocol.MessageType {
	return protocol.DropDatabase
}

func (d *DropDatabaseStatement) Serialize() ([]byte, error) {
	msgpackBytes, err := msgpack.Marshal(d)
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

func (d *DropDatabaseStatement) Deserialize(data []byte) error {
	if len(data) < 4 {
		return NewInvalidMessagePackDataError()
	}

	length := int(data[0])<<24 | int(data[1])<<16 | int(data[2])<<8 | int(data[3])

	if len(data[4:]) != length {
		return NewInvalidMessagePackDataError()
	}

	err := msgpack.Unmarshal(data[4:], d)
	if err != nil {
		return err
	}

	return nil
}
