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
	UseDatabase    MessageType = 4

	// Table Operations
	CreateTable   MessageType = 10
	DropTable     MessageType = 11
	AlterTable    MessageType = 12
	RenameTable   MessageType = 13
	TruncateTable MessageType = 14
	ShowTables    MessageType = 15
	DescribeTable MessageType = 16
	CopyTable     MessageType = 17

	// Index Operations
	CreateIndex  MessageType = 20
	DropIndex    MessageType = 21
	ShowIndexes  MessageType = 22
	RebuildIndex MessageType = 23

	// Data Operations
	Insert     MessageType = 30
	Select     MessageType = 31
	Update     MessageType = 32
	Delete     MessageType = 33
	BulkInsert MessageType = 34
	Upsert     MessageType = 35
	Query      MessageType = 36

	// Transaction Management
	BeginTransaction MessageType = 40
	Commit           MessageType = 41
	Rollback         MessageType = 42
	Savepoint        MessageType = 43
	ReleaseSavepoint MessageType = 44

	// Replication & Synchronization
	MasterConnected   MessageType = 60
	SlaveConnected    MessageType = 61
	StartReplication  MessageType = 62
	StopReplication   MessageType = 63
	SyncData          MessageType = 64
	ReplicationStatus MessageType = 65
	ReplicationLag    MessageType = 66
	PromoteToMaster   MessageType = 67
	DemoteToSlave     MessageType = 68

	// Authentication & User Management
	Login           MessageType = 50
	Logout          MessageType = 51
	CreateUser      MessageType = 52
	DropUser        MessageType = 53
	GrantPrivilege  MessageType = 54
	RevokePrivilege MessageType = 55
	ChangePassword  MessageType = 56

	// Utility Commands
	Ping           MessageType = 90
	Pong           MessageType = 91
	Greeting       MessageType = 92
	Welcome        MessageType = 93
	UnknownCommand MessageType = 255

	// Cluster Management
	JoinCluster   MessageType = 100
	LeaveCluster  MessageType = 101
	ClusterStatus MessageType = 102
	ElectLeader   MessageType = 103

	// Backup & Restore
	StartBackup  MessageType = 110
	StopBackup   MessageType = 111
	Restore      MessageType = 112
	BackupStatus MessageType = 113

	// Monitoring & Metrics
	GetMetrics  MessageType = 120
	GetLogs     MessageType = 121
	HealthCheck MessageType = 122

	// Configuration Management
	SetConfig    MessageType = 130
	GetConfig    MessageType = 131
	ReloadConfig MessageType = 132

	// Custom Commands
	CustomCommand MessageType = 200
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
	UseDatabase:    "UseDatabase",

	// Table Operations
	CreateTable:   "CreateTable",
	DropTable:     "DropTable",
	AlterTable:    "AlterTable",
	RenameTable:   "RenameTable",
	TruncateTable: "TruncateTable",
	ShowTables:    "ShowTables",
	DescribeTable: "DescribeTable",
	CopyTable:     "CopyTable",

	// Index Operations
	CreateIndex:  "CreateIndex",
	DropIndex:    "DropIndex",
	ShowIndexes:  "ShowIndexes",
	RebuildIndex: "RebuildIndex",

	// Data Operations
	Insert:     "Insert",
	Select:     "Select",
	Update:     "Update",
	Delete:     "Delete",
	BulkInsert: "BulkInsert",
	Upsert:     "Upsert",
	Query:      "Query",

	// Transaction Management
	BeginTransaction: "BeginTransaction",
	Commit:           "Commit",
	Rollback:         "Rollback",
	Savepoint:        "Savepoint",
	ReleaseSavepoint: "ReleaseSavepoint",

	// Replication & Synchronization
	MasterConnected:   "MasterConnected",
	SlaveConnected:    "SlaveConnected",
	StartReplication:  "StartReplication",
	StopReplication:   "StopReplication",
	SyncData:          "SyncData",
	ReplicationStatus: "ReplicationStatus",
	ReplicationLag:    "ReplicationLag",
	PromoteToMaster:   "PromoteToMaster",
	DemoteToSlave:     "DemoteToSlave",

	// Authentication & User Management
	Login:           "Login",
	Logout:          "Logout",
	CreateUser:      "CreateUser",
	DropUser:        "DropUser",
	GrantPrivilege:  "GrantPrivilege",
	RevokePrivilege: "RevokePrivilege",
	ChangePassword:  "ChangePassword",

	// Utility Commands
	Ping:           "Ping",
	Pong:           "Pong",
	Greeting:       "Greeting",
	Welcome:        "Welcome",
	UnknownCommand: "UnknownCommand",

	// Cluster Management
	JoinCluster:   "JoinCluster",
	LeaveCluster:  "LeaveCluster",
	ClusterStatus: "ClusterStatus",
	ElectLeader:   "ElectLeader",

	// Backup & Restore
	StartBackup:  "StartBackup",
	StopBackup:   "StopBackup",
	Restore:      "Restore",
	BackupStatus: "BackupStatus",

	// Monitoring & Metrics
	GetMetrics:  "GetMetrics",
	GetLogs:     "GetLogs",
	HealthCheck: "HealthCheck",

	// Configuration Management
	SetConfig:    "SetConfig",
	GetConfig:    "GetConfig",
	ReloadConfig: "ReloadConfig",

	// Custom Commands
	CustomCommand: "CustomCommand",
}

// String returns the name of the MessageType.
func (mt MessageType) String() string {
	if name, ok := messageTypeNames[mt]; ok {
		return name
	}
	return fmt.Sprintf("UnknownMessageType(%d)", mt)
}
