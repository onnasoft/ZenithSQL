package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type DemoteToSlaveStatement struct {
	NodeID string `msgpack:"node_id"` // Identificador único del nodo a degradar
}

func NewDemoteToSlaveStatement(nodeID string) (*DemoteToSlaveStatement, error) {
	stmt := &DemoteToSlaveStatement{
		NodeID: nodeID,
	}

	return stmt, nil
}

func (d DemoteToSlaveStatement) Protocol() protocol.MessageType {
	return protocol.DemoteToSlave
}

func (d DemoteToSlaveStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(d)
}

func (d *DemoteToSlaveStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, d)
}

func (d DemoteToSlaveStatement) String() string {
	return fmt.Sprintf("DemoteToSlaveStatement{NodeID: %s}", d.NodeID)
}
