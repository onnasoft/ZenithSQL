package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type LeaveClusterStatement struct {
	NodeID    string `msgpack:"node_id"`
	ClusterID string `msgpack:"cluster_id"`
}

func NewLeaveClusterStatement(nodeID, clusterID string) (*LeaveClusterStatement, error) {
	stmt := &LeaveClusterStatement{
		NodeID:    nodeID,
		ClusterID: clusterID,
	}

	return stmt, nil
}

func (l LeaveClusterStatement) Protocol() protocol.MessageType {
	return protocol.LeaveCluster
}

func (l LeaveClusterStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(l)
}

func (l *LeaveClusterStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, l)
}

func (l LeaveClusterStatement) String() string {
	return fmt.Sprintf("LeaveClusterStatement{NodeID: %s, ClusterID: %s}", l.NodeID, l.ClusterID)
}
