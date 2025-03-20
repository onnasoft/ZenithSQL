package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type GetConfigStatement struct {
	ConfigKey string `msgpack:"config_key"` // Clave de configuración
}

func NewGetConfigStatement(configKey string) (*GetConfigStatement, error) {
	stmt := &GetConfigStatement{
		ConfigKey: configKey,
	}

	return stmt, nil
}

func (g GetConfigStatement) Protocol() protocol.MessageType {
	return protocol.GetConfig
}

func (g GetConfigStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(g)
}

func (g *GetConfigStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, g)
}

func (g GetConfigStatement) String() string {
	return fmt.Sprintf("GetConfigStatement{ConfigKey: %s}", g.ConfigKey)
}
