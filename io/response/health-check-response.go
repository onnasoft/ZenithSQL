package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type HealthCheckResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
	Status  string `msgpack:"status"`
}

func NewHealthCheckResponse(success bool, message, status string) *HealthCheckResponse {
	return &HealthCheckResponse{
		Success: success,
		Message: message,
		Status:  status,
	}
}

func (r *HealthCheckResponse) IsSuccess() bool {
	return r.Success
}

func (r *HealthCheckResponse) GetMessage() string {
	return r.Message
}

func (r *HealthCheckResponse) Protocol() protocol.MessageType {
	return protocol.HealthCheck
}

func (r *HealthCheckResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *HealthCheckResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *HealthCheckResponse) String() string {
	return fmt.Sprintf("HealthCheckResponse{Success: %t, Message: %s, Status: %s}", r.Success, r.Message, r.Status)
}
