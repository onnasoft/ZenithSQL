package statement

import (
	"github.com/onnasoft/ZenithSQL/protocol"
)

type Statement interface {
	Protocol() protocol.MessageType
	ToBytes() ([]byte, error)
	FromBytes(data []byte) error
	String() string
}

func DeserializeStatement(messageType protocol.MessageType, data []byte) (Statement, error) {
	var stmt Statement

	switch messageType {
	// Database Management
	case protocol.CreateDatabase:
		stmt = &CreateDatabaseStatement{}
	case protocol.DropDatabase:
		stmt = &DropDatabaseStatement{}

	// Table Operations
	case protocol.CreateTable:
		stmt = &CreateTableStatement{}
	case protocol.DropTable:
		stmt = &DropTableStatement{}
	case protocol.AlterTable:
		stmt = &AlterTableStatement{}
	case protocol.RenameTable:
		stmt = &RenameTableStatement{}
	case protocol.TruncateTable:
		stmt = &TruncateTableStatement{}
	case protocol.ShowTables:
		stmt = &EmptyStatement{MessageType: protocol.ShowTables}
	case protocol.DescribeTable:
		stmt = &DescribeTableStatement{}

	// Index Operations
	case protocol.CreateIndex:
		stmt = &CreateIndexStatement{}
	case protocol.DropIndex:
		stmt = &DropIndexStatement{}
	case protocol.ShowIndexes:
		stmt = &ShowIndexesStatement{}

	// Data Operations
	case protocol.Insert:
		stmt = &InsertStatement{}
	case protocol.Select:
		stmt = &SelectStatement{}
	case protocol.Update:
		stmt = &UpdateStatement{}
	case protocol.Delete:
		stmt = &DeleteStatement{}
	case protocol.BulkInsert:
		stmt = &BulkInsertStatement{}
	case protocol.Upsert:
		stmt = &UpsertStatement{}

	// Transaction Management
	case protocol.BeginTransaction:
		stmt = &BeginTransactionStatement{}
	case protocol.Commit:
		stmt = &CommitStatement{}
	case protocol.Rollback:
		stmt = &RollbackStatement{}
	case protocol.Savepoint:
		stmt = &SavepointStatement{}
	case protocol.ReleaseSavepoint:
		stmt = &ReleaseSavepointStatement{}

	// Authentication & User Management
	case protocol.Login:
		stmt = &LoginStatement{}

	// Utility Commands
	case protocol.Ping:
		stmt = &EmptyStatement{MessageType: protocol.Ping}
	case protocol.Pong:
		stmt = &EmptyStatement{MessageType: protocol.Pong}
	case protocol.Greeting:
		stmt = &EmptyStatement{MessageType: protocol.Greeting}
	case protocol.Welcome:
		stmt = &EmptyStatement{MessageType: protocol.Welcome}

	default:
		return nil, NewErrUnsupportedStatement()
	}

	return stmt, stmt.FromBytes(data)
}
