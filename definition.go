package sqlparser

import (
	"github.com/OnnaSoft/sql-parser/protocol"
)

type Statement interface {
	Protocol() protocol.MessageType
	ToBytes() ([]byte, error)
	FromBytes(data []byte) error
}
