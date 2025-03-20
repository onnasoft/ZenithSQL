package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type PromoteToMasterStatement struct {
	NodeID string `msgpack:"node_id"` // Identificador único del nodo a promover
}

func NewPromoteToMasterStatement(nodeID string) (*PromoteToMasterStatement, error) {
	stmt := &PromoteToMasterStatement{
		NodeID: nodeID,
	}

	return stmt, nil
}

func (p PromoteToMasterStatement) Protocol() protocol.MessageType {
	return protocol.PromoteToMaster
}

func (p PromoteToMasterStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(p)
}

func (p *PromoteToMasterStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, p)
}

func (p PromoteToMasterStatement) String() string {
	return fmt.Sprintf("PromoteToMasterStatement{NodeID: %s}", p.NodeID)
}
