package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type StartReplicationStatement struct {
	ReplicationID string `msgpack:"replication_id"` // Identificador único de la replicación
}

func NewStartReplicationStatement(replicationID string) (*StartReplicationStatement, error) {
	stmt := &StartReplicationStatement{
		ReplicationID: replicationID,
	}

	return stmt, nil
}

func (s StartReplicationStatement) Protocol() protocol.MessageType {
	return protocol.StartReplication
}

func (s StartReplicationStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(s)
}

func (s *StartReplicationStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, s)
}

func (s StartReplicationStatement) String() string {
	return fmt.Sprintf("StartReplicationStatement{ReplicationID: %s}", s.ReplicationID)
}
