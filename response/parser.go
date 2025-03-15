package response

import (
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/onnasoft/ZenithSQL/statement"
)

type Response interface {
	IsSuccess() bool
	GetMessage() string
	statement.Statement
}

func DeserializeResponse(messageType protocol.MessageType, data []byte) (Response, error) {
	var resp Response

	switch messageType {
	// Database Management
	case protocol.CreateDatabase:
		resp = &CreateDatabaseResponse{}
	case protocol.DropDatabase:
		resp = &DropDatabaseResponse{}

	// Table Operations
	case protocol.CreateTable:
		resp = &CreateTableResponse{}
	case protocol.DropTable:
		resp = &DropTableResponse{}
	case protocol.AlterTable:
		resp = &AlterTableResponse{}
	case protocol.RenameTable:
		resp = &RenameTableResponse{}
	case protocol.TruncateTable:
		resp = &TruncateTableResponse{}
	case protocol.ShowTables:
		resp = &ShowTablesResponse{}
	case protocol.DescribeTable:
		resp = &DescribeTableResponse{}

	// Index Operations
	case protocol.CreateIndex:
		resp = &CreateIndexResponse{}
	case protocol.DropIndex:
		resp = &DropIndexResponse{}
	case protocol.ShowIndexes:
		resp = &ShowIndexesResponse{}

	// Data Operations
	case protocol.Insert:
		resp = &InsertResponse{}
	case protocol.Select:
		resp = &SelectResponse{}
	case protocol.Update:
		resp = &UpdateResponse{}
	case protocol.Delete:
		resp = &DeleteResponse{}
	case protocol.BulkInsert:
		resp = &BulkInsertResponse{}
	case protocol.Upsert:
		resp = &UpsertResponse{}

	// Transaction Management
	case protocol.BeginTransaction:
		resp = &BeginTransactionResponse{}
	case protocol.Commit:
		resp = &CommitResponse{}
	case protocol.Rollback:
		resp = &RollbackResponse{}
	case protocol.Savepoint:
		resp = &SavepointResponse{}
	case protocol.ReleaseSavepoint:
		resp = &ReleaseSavepointResponse{}

	// Authentication & User Management
	case protocol.Login:
		resp = &LoginResponse{}

	// Utility Commands
	case protocol.Ping:
		resp = &PingResponse{}
	case protocol.Pong:
		resp = &PongResponse{}
	case protocol.Greeting:
		resp = &GreetingResponse{}
	case protocol.Welcome:
		resp = &WelcomeResponse{}

	default:
		return nil, NewErrUnsupportedResponse()
	}

	return resp, resp.FromBytes(data)
}
