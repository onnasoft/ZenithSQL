package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
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

func (e *EmptyStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(e)
}

func (e *EmptyStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, e)
}

func (e *EmptyStatement) String() string {
	return fmt.Sprintf("EmptyStatement{MessageType: %s}", e.MessageType)
}
