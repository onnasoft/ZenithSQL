package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type MasterConnectedStatement struct {
	MasterID string `msgpack:"master_id"` // Identificador único del maestro
}

func NewMasterConnectedStatement(masterID string) (*MasterConnectedStatement, error) {
	stmt := &MasterConnectedStatement{
		MasterID: masterID,
	}

	return stmt, nil
}

func (m MasterConnectedStatement) Protocol() protocol.MessageType {
	return protocol.MasterConnected
}

func (m MasterConnectedStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(m)
}

func (m *MasterConnectedStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, m)
}

func (m MasterConnectedStatement) String() string {
	return fmt.Sprintf("MasterConnectedStatement{MasterID: %s}", m.MasterID)
}
