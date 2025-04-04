package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type BackupStatusStatement struct {
	BackupID string `msgpack:"backup_id"` // Identificador único del backup
}

func NewBackupStatusStatement(backupID string) (*BackupStatusStatement, error) {
	stmt := &BackupStatusStatement{
		BackupID: backupID,
	}

	return stmt, nil
}

func (b BackupStatusStatement) Protocol() protocol.MessageType {
	return protocol.BackupStatus
}

func (b BackupStatusStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(b)
}

func (b *BackupStatusStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, b)
}

func (b BackupStatusStatement) String() string {
	return fmt.Sprintf("BackupStatusStatement{BackupID: %s}", b.BackupID)
}
