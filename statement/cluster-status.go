package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ClusterStatusStatement struct {
	ClusterID string `msgpack:"cluster_id"`
}

func NewClusterStatusStatement(clusterID string) (*ClusterStatusStatement, error) {
	stmt := &ClusterStatusStatement{
		ClusterID: clusterID,
	}

	return stmt, nil
}

func (c ClusterStatusStatement) Protocol() protocol.MessageType {
	return protocol.ClusterStatus
}

func (c ClusterStatusStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(c)
}

func (c *ClusterStatusStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, c)
}

func (c ClusterStatusStatement) String() string {
	return fmt.Sprintf("ClusterStatusStatement{ClusterID: %s}", c.ClusterID)
}
