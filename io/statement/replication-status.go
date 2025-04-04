package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ReplicationStatusStatement struct {
	ReplicationID string `msgpack:"replication_id"`
}

func NewReplicationStatusStatement(replicationID string) (*ReplicationStatusStatement, error) {
	stmt := &ReplicationStatusStatement{
		ReplicationID: replicationID,
	}

	return stmt, nil
}

func (r ReplicationStatusStatement) Protocol() protocol.MessageType {
	return protocol.ReplicationStatus
}

func (r ReplicationStatusStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *ReplicationStatusStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r ReplicationStatusStatement) String() string {
	return fmt.Sprintf("ReplicationStatusStatement{ReplicationID: %s}", r.ReplicationID)
}
