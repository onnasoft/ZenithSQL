package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type RestoreStatement struct {
	BackupID string `msgpack:"backup_id"` // Identificador único del backup
}

func NewRestoreStatement(backupID string) (*RestoreStatement, error) {
	stmt := &RestoreStatement{
		BackupID: backupID,
	}

	return stmt, nil
}

func (r RestoreStatement) Protocol() protocol.MessageType {
	return protocol.Restore
}

func (r RestoreStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *RestoreStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r RestoreStatement) String() string {
	return fmt.Sprintf("RestoreStatement{BackupID: %s}", r.BackupID)
}
