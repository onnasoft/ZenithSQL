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
	MasterConnected   MessageType = 60 // Notificación de que el maestro se ha conectado
	SlaveConnected    MessageType = 61 // Notificación de que un esclavo se ha conectado
	StartReplication  MessageType = 62 // Iniciar replicación
	StopReplication   MessageType = 63 // Detener replicación
	SyncData          MessageType = 64 // Sincronizar datos entre maestro y esclavo
	ReplicationStatus MessageType = 65 // Obtener el estado de la replicación
	ReplicationLag    MessageType = 66 // Obtener el retraso de replicación
	PromoteToMaster   MessageType = 67 // Promover un esclavo a maestro
	DemoteToSlave     MessageType = 68 // Degradar un maestro a esclavo

	// Authentication & User Management
	Login           MessageType = 50
	Logout          MessageType = 51 // Cerrar sesión
	CreateUser      MessageType = 52 // Crear un nuevo usuario
	DropUser        MessageType = 53 // Eliminar un usuario
	GrantPrivilege  MessageType = 54 // Otorgar privilegios a un usuario
	RevokePrivilege MessageType = 55 // Revocar privilegios de un usuario
	ChangePassword  MessageType = 56 // Cambiar contraseña de usuario

	// Utility Commands
	Ping           MessageType = 90
	Pong           MessageType = 91
	Greeting       MessageType = 92
	Welcome        MessageType = 93
	UnknownCommand MessageType = 255

	// Cluster Management
	JoinCluster   MessageType = 100 // Unirse a un clúster
	LeaveCluster  MessageType = 101 // Abandonar un clúster
	ClusterStatus MessageType = 102 // Obtener el estado del clúster
	ElectLeader   MessageType = 103 // Elegir un nuevo líder en el clúster

	// Backup & Restore
	StartBackup  MessageType = 110 // Iniciar un backup
	StopBackup   MessageType = 111 // Detener un backup
	Restore      MessageType = 112 // Restaurar desde un backup
	BackupStatus MessageType = 113 // Obtener el estado del backup

	// Monitoring & Metrics
	GetMetrics  MessageType = 120 // Obtener métricas del sistema
	GetLogs     MessageType = 121 // Obtener registros (logs) del sistema
	HealthCheck MessageType = 122 // Verificar el estado de salud del sistema

	// Configuration Management
	SetConfig    MessageType = 130 // Establecer configuración
	GetConfig    MessageType = 131 // Obtener configuración
	ReloadConfig MessageType = 132 // Recargar configuración

	// Custom Commands
	CustomCommand MessageType = 200 // Comando personalizado (para extensiones)
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

	// Authentication & User Management
	Login: "Login",

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
