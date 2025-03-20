package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type SyncDataStatement struct {
	Data []byte `msgpack:"data"` // Datos a sincronizar
}

func NewSyncDataStatement(data []byte) (*SyncDataStatement, error) {
	stmt := &SyncDataStatement{
		Data: data,
	}

	return stmt, nil
}

func (s SyncDataStatement) Protocol() protocol.MessageType {
	return protocol.SyncData
}

func (s SyncDataStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(s)
}

func (s *SyncDataStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, s)
}

func (s SyncDataStatement) String() string {
	return fmt.Sprintf("SyncDataStatement{Data: %v}", s.Data)
}
