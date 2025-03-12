package statement

import (
	"github.com/onnasoft/sql-parser/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type EmptyStatement struct {
	MessageType protocol.MessageType
}

func NewEmptyStatement(msgType protocol.MessageType) *EmptyStatement {
	return &EmptyStatement{
		MessageType: msgType,
	}
}

func (e *EmptyStatement) Protocol() protocol.MessageType {
	return e.MessageType
}

func (e *EmptyStatement) Serialize() ([]byte, error) {
	return msgpack.Marshal(e)
}

func (e *EmptyStatement) Deserialize(data []byte) error {
	return msgpack.Unmarshal(data, e)
}
