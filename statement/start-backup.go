package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type StartBackupStatement struct {
	BackupID string `msgpack:"backup_id"` // Identificador único del backup
}

func NewStartBackupStatement(backupID string) (*StartBackupStatement, error) {
	stmt := &StartBackupStatement{
		BackupID: backupID,
	}

	return stmt, nil
}

func (s StartBackupStatement) Protocol() protocol.MessageType {
	return protocol.StartBackup
}

func (s StartBackupStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(s)
}

func (s *StartBackupStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, s)
}

func (s StartBackupStatement) String() string {
	return fmt.Sprintf("StartBackupStatement{BackupID: %s}", s.BackupID)
}
