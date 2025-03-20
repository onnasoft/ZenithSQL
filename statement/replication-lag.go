package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ReplicationLagStatement struct {
	ReplicationID string `msgpack:"replication_id"` // Identificador único de la replicación
}

func NewReplicationLagStatement(replicationID string) (*ReplicationLagStatement, error) {
	stmt := &ReplicationLagStatement{
		ReplicationID: replicationID,
	}

	return stmt, nil
}

func (r ReplicationLagStatement) Protocol() protocol.MessageType {
	return protocol.ReplicationLag
}

func (r ReplicationLagStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *ReplicationLagStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r ReplicationLagStatement) String() string {
	return fmt.Sprintf("ReplicationLagStatement{ReplicationID: %s}", r.ReplicationID)
}
