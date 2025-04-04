package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type HealthCheckStatement struct {
	CheckType string `msgpack:"check_type"` // Tipo de verificación (por ejemplo, "full", "quick")
}

func NewHealthCheckStatement(checkType string) (*HealthCheckStatement, error) {
	stmt := &HealthCheckStatement{
		CheckType: checkType,
	}

	return stmt, nil
}

func (h HealthCheckStatement) Protocol() protocol.MessageType {
	return protocol.HealthCheck
}

func (h HealthCheckStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(h)
}

func (h *HealthCheckStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, h)
}

func (h HealthCheckStatement) String() string {
	return fmt.Sprintf("HealthCheckStatement{CheckType: %s}", h.CheckType)
}
