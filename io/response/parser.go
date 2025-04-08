package response

import (
	"github.com/onnasoft/ZenithSQL/io/protocol"
)

type Response interface {
	IsSuccess() bool
	GetMessage() string
	Protocol() protocol.MessageType
	ToBytes() ([]byte, error)
	FromBytes(data []byte) error
	String() string
}

type responseConstructor func() Response

var responseMap = map[protocol.MessageType]responseConstructor{
	// Database Management
	protocol.CreateDatabase: func() Response { return &CreateDatabaseResponse{} },
	protocol.DropDatabase:   func() Response { return &DropDatabaseResponse{} },
	protocol.ShowDatabases:  func() Response { return &ShowDatabasesResponse{} },
	protocol.UseDatabase:    func() Response { return &UseDatabaseResponse{} },

	// Table Operations
	protocol.CreateTable:   func() Response { return &CreateTableResponse{} },
	protocol.DropTable:     func() Response { return &DropTableResponse{} },
	protocol.AlterTable:    func() Response { return &AlterTableResponse{} },
	protocol.RenameTable:   func() Response { return &RenameTableResponse{} },
	protocol.TruncateTable: func() Response { return &TruncateTableResponse{} },
	protocol.ShowTables:    func() Response { return &ShowTablesResponse{} },
	protocol.DescribeTable: func() Response { return &DescribeTableResponse{} },
	protocol.CopyTable:     func() Response { return &CopyTableResponse{} },

	// Index Operations
	protocol.CreateIndex:  func() Response { return &CreateIndexResponse{} },
	protocol.DropIndex:    func() Response { return &DropIndexResponse{} },
	protocol.ShowIndexes:  func() Response { return &ShowIndexesResponse{} },
	protocol.RebuildIndex: func() Response { return &RebuildIndexResponse{} },

	// Data Operations
	protocol.Insert:     func() Response { return &InsertResponse{} },
	protocol.Select:     func() Response { return &SelectResponse{} },
	protocol.Update:     func() Response { return &UpdateResponse{} },
	protocol.Delete:     func() Response { return &DeleteResponse{} },
	protocol.BulkInsert: func() Response { return &BulkInsertResponse{} },
	protocol.Upsert:     func() Response { return &UpsertResponse{} },
	protocol.Query:      func() Response { return &QueryResponse{} },

	// Transaction Management
	protocol.BeginTransaction: func() Response { return &BeginTransactionResponse{} },
	protocol.Commit:           func() Response { return &CommitResponse{} },
	protocol.Rollback:         func() Response { return &RollbackResponse{} },
	protocol.Savepoint:        func() Response { return &SavepointResponse{} },
	protocol.ReleaseSavepoint: func() Response { return &ReleaseSavepointResponse{} },

	// Replication & Synchronization
	protocol.MasterConnected:   func() Response { return &MasterConnectedResponse{} },
	protocol.SlaveConnected:    func() Response { return &SlaveConnectedResponse{} },
	protocol.StartReplication:  func() Response { return &StartReplicationResponse{} },
	protocol.StopReplication:   func() Response { return &StopReplicationResponse{} },
	protocol.SyncData:          func() Response { return &SyncDataResponse{} },
	protocol.ReplicationStatus: func() Response { return &ReplicationStatusResponse{} },
	protocol.ReplicationLag:    func() Response { return &ReplicationLagResponse{} },
	protocol.PromoteToMaster:   func() Response { return &PromoteToMasterResponse{} },
	protocol.DemoteToSlave:     func() Response { return &DemoteToSlaveResponse{} },

	// Authentication & User Management
	protocol.Login:           func() Response { return &LoginResponse{} },
	protocol.Logout:          func() Response { return &LogoutResponse{} },
	protocol.CreateUser:      func() Response { return &CreateUserResponse{} },
	protocol.DropUser:        func() Response { return &DropUserResponse{} },
	protocol.GrantPrivilege:  func() Response { return &GrantPrivilegeResponse{} },
	protocol.RevokePrivilege: func() Response { return &RevokePrivilegeResponse{} },
	protocol.ChangePassword:  func() Response { return &ChangePasswordResponse{} },

	// Utility Commands
	protocol.Ping:     func() Response { return &PingResponse{} },
	protocol.Pong:     func() Response { return &PongResponse{} },
	protocol.Greeting: func() Response { return &GreetingResponse{} },
	protocol.Welcome:  func() Response { return &WelcomeResponse{} },

	// Cluster Management
	protocol.JoinCluster:   func() Response { return &JoinClusterResponse{} },
	protocol.LeaveCluster:  func() Response { return &LeaveClusterResponse{} },
	protocol.ClusterStatus: func() Response { return &ClusterStatusResponse{} },
	protocol.ElectLeader:   func() Response { return &ElectLeaderResponse{} },

	// Backup & Restore
	protocol.StartBackup:  func() Response { return &StartBackupResponse{} },
	protocol.StopBackup:   func() Response { return &StopBackupResponse{} },
	protocol.Restore:      func() Response { return &RestoreResponse{} },
	protocol.BackupStatus: func() Response { return &BackupStatusResponse{} },

	// Monitoring & Metrics
	protocol.GetMetrics:  func() Response { return &GetMetricsResponse{} },
	protocol.GetLogs:     func() Response { return &GetLogsResponse{} },
	protocol.HealthCheck: func() Response { return &HealthCheckResponse{} },

	// Configuration Management
	protocol.SetConfig:    func() Response { return &SetConfigResponse{} },
	protocol.GetConfig:    func() Response { return &GetConfigResponse{} },
	protocol.ReloadConfig: func() Response { return &ReloadConfigResponse{} },

	// Custom Commands
	protocol.CustomCommand: func() Response { return &CustomCommandResponse{} },
}

// DeserializeResponse deserializa un mensaje en la respuesta correspondiente.
func Deserialize(messageType protocol.MessageType, data []byte) (Response, error) {
	constructor, ok := responseMap[messageType]
	if !ok {
		return nil, NewErrUnsupportedResponse()
	}

	resp := constructor()
	return resp, resp.FromBytes(data)
}
