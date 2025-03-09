package protocol

import (
	"fmt"
)

type MessageType uint32

const (
	// Database Management
	CreateDatabase MessageType = 1
	DropDatabase   MessageType = 2
	ShowDatabases  MessageType = 3

	// Table Operations
	CreateTable   MessageType = 10
	DropTable     MessageType = 11
	AlterTable    MessageType = 12
	RenameTable   MessageType = 13
	TruncateTable MessageType = 14
	ShowTables    MessageType = 15
	DescribeTable MessageType = 16

	// Index Operations
	CreateIndex MessageType = 20
	DropIndex   MessageType = 21
	ShowIndexes MessageType = 22

	// Data Operations
	Insert     MessageType = 30
	Select     MessageType = 31
	Update     MessageType = 32
	Delete     MessageType = 33
	BulkInsert MessageType = 34
	Upsert     MessageType = 35

	// Transaction Management
	BeginTransaction MessageType = 40
	Commit           MessageType = 41
	Rollback         MessageType = 42
	Savepoint        MessageType = 43
	ReleaseSavepoint MessageType = 44

	// Utility Commands
	Ping           MessageType = 90
	Pong           MessageType = 91
	Greeting       MessageType = 92
	Welcome        MessageType = 93
	UnknownCommand MessageType = 255
)

var messageTypeNamesLookup = map[string]MessageType{}

func init() {
	for mt, n := range messageTypeNames {
		messageTypeNamesLookup[n] = mt
	}
}

func GetMessageTypeFromID(id uint32) MessageType {
	return MessageType(id)
}

func GetMessageTypeFromName(name string) MessageType {
	if mt, ok := messageTypeNamesLookup[name]; ok {
		return mt
	}
	return UnknownCommand
}

// messageTypeNames maps MessageType values to their string representations.
var messageTypeNames = map[MessageType]string{
	// Database Management
	CreateDatabase: "CreateDatabase",
	DropDatabase:   "DropDatabase",
	ShowDatabases:  "ShowDatabases",

	// Table Operations
	CreateTable:   "CreateTable",
	DropTable:     "DropTable",
	AlterTable:    "AlterTable",
	RenameTable:   "RenameTable",
	TruncateTable: "TruncateTable",
	ShowTables:    "ShowTables",
	DescribeTable: "DescribeTable",

	// Index Operations
	CreateIndex: "CreateIndex",
	DropIndex:   "DropIndex",
	ShowIndexes: "ShowIndexes",

	// Data Operations
	Insert:     "Insert",
	Select:     "Select",
	Update:     "Update",
	Delete:     "Delete",
	BulkInsert: "BulkInsert",
	Upsert:     "Upsert",

	// Transaction Management
	BeginTransaction: "BeginTransaction",
	Commit:           "Commit",
	Rollback:         "Rollback",
	Savepoint:        "Savepoint",
	ReleaseSavepoint: "ReleaseSavepoint",

	// Utility Commands
	Ping:           "Ping",
	Pong:           "Pong",
	Greeting:       "Greeting",
	Welcome:        "Welcome",
	UnknownCommand: "UnknownCommand",
}

// String returns the name of the MessageType.
func (mt MessageType) String() string {
	if name, ok := messageTypeNames[mt]; ok {
		return name
	}
	return fmt.Sprintf("UnknownMessageType(%d)", mt)
}
