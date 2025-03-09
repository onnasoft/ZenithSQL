package sqlparser

import (
	"github.com/onnasoft/sql-parser/protocol"
)

type Statement interface {
	Protocol() protocol.MessageType
	Serialize() ([]byte, error)
	Deserialize(data []byte) error
}
