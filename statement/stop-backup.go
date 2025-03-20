package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type StopBackupStatement struct {
	BackupID string `msgpack:"backup_id"` // Identificador único del backup
}

func NewStopBackupStatement(backupID string) (*StopBackupStatement, error) {
	stmt := &StopBackupStatement{
		BackupID: backupID,
	}

	return stmt, nil
}

func (s StopBackupStatement) Protocol() protocol.MessageType {
	return protocol.StopBackup
}

func (s StopBackupStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(s)
}

func (s *StopBackupStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, s)
}

func (s StopBackupStatement) String() string {
	return fmt.Sprintf("StopBackupStatement{BackupID: %s}", s.BackupID)
}
