package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type SetConfigStatement struct {
	ConfigKey   string `msgpack:"config_key"`   // Clave de configuración
	ConfigValue string `msgpack:"config_value"` // Valor de configuración
}

func NewSetConfigStatement(configKey, configValue string) (*SetConfigStatement, error) {
	stmt := &SetConfigStatement{
		ConfigKey:   configKey,
		ConfigValue: configValue,
	}

	return stmt, nil
}

func (s SetConfigStatement) Protocol() protocol.MessageType {
	return protocol.SetConfig
}

func (s SetConfigStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(s)
}

func (s *SetConfigStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, s)
}

func (s SetConfigStatement) String() string {
	return fmt.Sprintf("SetConfigStatement{ConfigKey: %s, ConfigValue: %s}", s.ConfigKey, s.ConfigValue)
}
