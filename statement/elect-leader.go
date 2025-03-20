package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ElectLeaderStatement struct {
	ClusterID string `msgpack:"cluster_id"`
	NodeID    string `msgpack:"node_id"`
}

func NewElectLeaderStatement(clusterID, nodeID string) (*ElectLeaderStatement, error) {
	stmt := &ElectLeaderStatement{
		ClusterID: clusterID,
		NodeID:    nodeID,
	}

	return stmt, nil
}

func (e ElectLeaderStatement) Protocol() protocol.MessageType {
	return protocol.ElectLeader
}

func (e ElectLeaderStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(e)
}

func (e *ElectLeaderStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, e)
}

func (e ElectLeaderStatement) String() string {
	return fmt.Sprintf("ElectLeaderStatement{ClusterID: %s, NodeID: %s}", e.ClusterID, e.NodeID)
}
