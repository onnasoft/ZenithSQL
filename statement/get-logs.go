package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type GetLogsStatement struct {
	LogLevel string `msgpack:"log_level"`
}

func NewGetLogsStatement(logLevel string) (*GetLogsStatement, error) {
	stmt := &GetLogsStatement{
		LogLevel: logLevel,
	}

	return stmt, nil
}

func (g GetLogsStatement) Protocol() protocol.MessageType {
	return protocol.GetLogs
}

func (g GetLogsStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(g)
}

func (g *GetLogsStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, g)
}

func (g GetLogsStatement) String() string {
	return fmt.Sprintf("GetLogsStatement{LogLevel: %s}", g.LogLevel)
}
