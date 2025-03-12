package statement

import (
	"github.com/onnasoft/sql-parser/protocol"
)

type Statement interface {
	Protocol() protocol.MessageType
	Serialize() ([]byte, error)
	Deserialize(data []byte) error
}

func DeserializeStatement(messageType protocol.MessageType, data []byte) (Statement, error) {
	var stmt Statement
	switch messageType {
	case protocol.Login:
		stmt = &LoginStatement{}
	case protocol.Ping:
		stmt = &EmptyStatement{MessageType: protocol.Ping}
	case protocol.Pong:
		stmt = &EmptyStatement{MessageType: protocol.Pong}
	case protocol.CreateDatabase:
		stmt = &CreateDatabaseStatement{}
	case protocol.CreateTable:
		stmt = &CreateTableStatement{}
	case protocol.DropDatabase:
		stmt = &DropDatabaseStatement{}
	default:
		return nil, NewErrUnsupportedStatement()
	}

	return stmt, stmt.Deserialize(data)
}
