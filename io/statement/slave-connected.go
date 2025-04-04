package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type SlaveConnectedStatement struct {
	SlaveID string `msgpack:"slave_id"` // Identificador único del esclavo
}

func NewSlaveConnectedStatement(slaveID string) (*SlaveConnectedStatement, error) {
	stmt := &SlaveConnectedStatement{
		SlaveID: slaveID,
	}

	return stmt, nil
}

func (s SlaveConnectedStatement) Protocol() protocol.MessageType {
	return protocol.SlaveConnected
}

func (s SlaveConnectedStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(s)
}

func (s *SlaveConnectedStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, s)
}

func (s SlaveConnectedStatement) String() string {
	return fmt.Sprintf("SlaveConnectedStatement{SlaveID: %s}", s.SlaveID)
}
