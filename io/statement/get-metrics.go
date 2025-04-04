package statement

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type GetMetricsStatement struct {
	MetricsType string `msgpack:"metrics_type"` // Tipo de métricas a obtener (por ejemplo, "cpu", "memory", "disk")
}

func NewGetMetricsStatement(metricsType string) (*GetMetricsStatement, error) {
	stmt := &GetMetricsStatement{
		MetricsType: metricsType,
	}

	return stmt, nil
}

func (g GetMetricsStatement) Protocol() protocol.MessageType {
	return protocol.GetMetrics
}

func (g GetMetricsStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(g)
}

func (g *GetMetricsStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, g)
}

func (g GetMetricsStatement) String() string {
	return fmt.Sprintf("GetMetricsStatement{MetricsType: %s}", g.MetricsType)
}
