package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type GetMetricsResponse struct {
	Success bool        `msgpack:"success"`
	Message string      `msgpack:"message"`
	Metrics interface{} `msgpack:"metrics"`
}

func NewGetMetricsResponse(success bool, message string, metrics interface{}) *GetMetricsResponse {
	return &GetMetricsResponse{
		Success: success,
		Message: message,
		Metrics: metrics,
	}
}

func (r *GetMetricsResponse) IsSuccess() bool {
	return r.Success
}

func (r *GetMetricsResponse) GetMessage() string {
	return r.Message
}

func (r *GetMetricsResponse) Protocol() protocol.MessageType {
	return protocol.GetMetrics
}

func (r *GetMetricsResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *GetMetricsResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *GetMetricsResponse) String() string {
	return fmt.Sprintf("GetMetricsResponse{Success: %t, Message: %s, Metrics: %v}", r.Success, r.Message, r.Metrics)
}
