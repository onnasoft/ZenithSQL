package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type StopReplicationStatement struct {
	ReplicationID string `msgpack:"replication_id"` // Identificador único de la replicación
}

func NewStopReplicationStatement(replicationID string) (*StopReplicationStatement, error) {
	stmt := &StopReplicationStatement{
		ReplicationID: replicationID,
	}

	return stmt, nil
}

func (s StopReplicationStatement) Protocol() protocol.MessageType {
	return protocol.StopReplication
}

func (s StopReplicationStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(s)
}

func (s *StopReplicationStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, s)
}

func (s StopReplicationStatement) String() string {
	return fmt.Sprintf("StopReplicationStatement{ReplicationID: %s}", s.ReplicationID)
}
