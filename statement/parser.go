package statement

import (
	"github.com/onnasoft/ZenithSQL/dto"
	"github.com/onnasoft/ZenithSQL/protocol"
)

type Statement interface {
	dto.Dto
}

type statementConstructor func() Statement

var statementMap = map[protocol.MessageType]statementConstructor{
	// Database Management
	protocol.CreateDatabase: func() Statement { return &CreateDatabaseStatement{} },
	protocol.DropDatabase:   func() Statement { return &DropDatabaseStatement{} },
	protocol.ShowDatabases:  func() Statement { return &EmptyStatement{MessageType: protocol.ShowDatabases} },
	protocol.UseDatabase:    func() Statement { return &UseDatabaseStatement{} },

	// Table Operations
	protocol.CreateTable:   func() Statement { return &CreateTableStatement{} },
	protocol.DropTable:     func() Statement { return &DropTableStatement{} },
	protocol.AlterTable:    func() Statement { return &AlterTableStatement{} },
	protocol.RenameTable:   func() Statement { return &RenameTableStatement{} },
	protocol.TruncateTable: func() Statement { return &TruncateTableStatement{} },
	protocol.ShowTables:    func() Statement { return &EmptyStatement{MessageType: protocol.ShowTables} },
	protocol.DescribeTable: func() Statement { return &DescribeTableStatement{} },
	protocol.CopyTable:     func() Statement { return &CopyTableStatement{} },

	// Index Operations
	protocol.CreateIndex:  func() Statement { return &CreateIndexStatement{} },
	protocol.DropIndex:    func() Statement { return &DropIndexStatement{} },
	protocol.ShowIndexes:  func() Statement { return &ShowIndexesStatement{} },
	protocol.RebuildIndex: func() Statement { return &RebuildIndexStatement{} },

	// Data Operations
	protocol.Insert:     func() Statement { return &InsertStatement{} },
	protocol.Select:     func() Statement { return &SelectStatement{} },
	protocol.Update:     func() Statement { return &UpdateStatement{} },
	protocol.Delete:     func() Statement { return &DeleteStatement{} },
	protocol.BulkInsert: func() Statement { return &BulkInsertStatement{} },
	protocol.Upsert:     func() Statement { return &UpsertStatement{} },
	protocol.Query:      func() Statement { return &QueryStatement{} },

	// Transaction Management
	protocol.BeginTransaction: func() Statement { return &BeginTransactionStatement{} },
	protocol.Commit:           func() Statement { return &CommitStatement{} },
	protocol.Rollback:         func() Statement { return &RollbackStatement{} },
	protocol.Savepoint:        func() Statement { return &SavepointStatement{} },
	protocol.ReleaseSavepoint: func() Statement { return &ReleaseSavepointStatement{} },

	// Replication & Synchronization
	protocol.MasterConnected:   func() Statement { return &MasterConnectedStatement{} },
	protocol.SlaveConnected:    func() Statement { return &SlaveConnectedStatement{} },
	protocol.StartReplication:  func() Statement { return &StartReplicationStatement{} },
	protocol.StopReplication:   func() Statement { return &StopReplicationStatement{} },
	protocol.SyncData:          func() Statement { return &SyncDataStatement{} },
	protocol.ReplicationStatus: func() Statement { return &ReplicationStatusStatement{} },
	protocol.ReplicationLag:    func() Statement { return &ReplicationLagStatement{} },
	protocol.PromoteToMaster:   func() Statement { return &PromoteToMasterStatement{} },
	protocol.DemoteToSlave:     func() Statement { return &DemoteToSlaveStatement{} },

	// Authentication & User Management
	protocol.Login:           func() Statement { return &LoginStatement{} },
	protocol.Logout:          func() Statement { return &LogoutStatement{} },
	protocol.CreateUser:      func() Statement { return &CreateUserStatement{} },
	protocol.DropUser:        func() Statement { return &DropUserStatement{} },
	protocol.GrantPrivilege:  func() Statement { return &GrantPrivilegeStatement{} },
	protocol.RevokePrivilege: func() Statement { return &RevokePrivilegeStatement{} },
	protocol.ChangePassword:  func() Statement { return &ChangePasswordStatement{} },

	// Utility Commands
	protocol.Ping:     func() Statement { return &EmptyStatement{MessageType: protocol.Ping} },
	protocol.Pong:     func() Statement { return &EmptyStatement{MessageType: protocol.Pong} },
	protocol.Greeting: func() Statement { return &EmptyStatement{MessageType: protocol.Greeting} },
	protocol.Welcome:  func() Statement { return &EmptyStatement{MessageType: protocol.Welcome} },

	// Cluster Management
	protocol.JoinCluster:   func() Statement { return &JoinClusterStatement{} },
	protocol.LeaveCluster:  func() Statement { return &LeaveClusterStatement{} },
	protocol.ClusterStatus: func() Statement { return &ClusterStatusStatement{} },
	protocol.ElectLeader:   func() Statement { return &ElectLeaderStatement{} },

	// Backup & Restore
	protocol.StartBackup:  func() Statement { return &StartBackupStatement{} },
	protocol.StopBackup:   func() Statement { return &StopBackupStatement{} },
	protocol.Restore:      func() Statement { return &RestoreStatement{} },
	protocol.BackupStatus: func() Statement { return &BackupStatusStatement{} },

	// Monitoring & Metrics
	protocol.GetMetrics:  func() Statement { return &GetMetricsStatement{} },
	protocol.GetLogs:     func() Statement { return &GetLogsStatement{} },
	protocol.HealthCheck: func() Statement { return &HealthCheckStatement{} },

	// Configuration Management
	protocol.SetConfig:    func() Statement { return &SetConfigStatement{} },
	protocol.GetConfig:    func() Statement { return &GetConfigStatement{} },
	protocol.ReloadConfig: func() Statement { return &ReloadConfigStatement{} },
}

func Deserialize(messageType protocol.MessageType, data []byte) (Statement, error) {
	constructor, ok := statementMap[messageType]
	if !ok {
		return nil, NewErrUnsupportedStatement()
	}

	stmt := constructor()
	return stmt, stmt.FromBytes(data)
}
