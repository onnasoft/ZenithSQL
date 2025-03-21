package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type RebuildIndexResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewRebuildIndexResponse(success bool, message string) *RebuildIndexResponse {
	return &RebuildIndexResponse{
		Success: success,
		Message: message,
	}
}

func (r *RebuildIndexResponse) IsSuccess() bool {
	return r.Success
}

func (r *RebuildIndexResponse) GetMessage() string {
	return r.Message
}

func (r *RebuildIndexResponse) Protocol() protocol.MessageType {
	return protocol.RebuildIndex
}

func (r *RebuildIndexResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *RebuildIndexResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *RebuildIndexResponse) String() string {
	return fmt.Sprintf("RebuildIndexResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
