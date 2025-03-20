package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ReloadConfigStatement struct {
	ConfigFile string `msgpack:"config_file"` // Archivo de configuración a recargar
}

func NewReloadConfigStatement(configFile string) (*ReloadConfigStatement, error) {
	stmt := &ReloadConfigStatement{
		ConfigFile: configFile,
	}

	return stmt, nil
}

func (r ReloadConfigStatement) Protocol() protocol.MessageType {
	return protocol.ReloadConfig
}

func (r ReloadConfigStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *ReloadConfigStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r ReloadConfigStatement) String() string {
	return fmt.Sprintf("ReloadConfigStatement{ConfigFile: %s}", r.ConfigFile)
}
