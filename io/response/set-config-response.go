package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type SetConfigResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewSetConfigResponse(success bool, message string) *SetConfigResponse {
	return &SetConfigResponse{
		Success: success,
		Message: message,
	}
}

func (r *SetConfigResponse) IsSuccess() bool {
	return r.Success
}

func (r *SetConfigResponse) GetMessage() string {
	return r.Message
}

func (r *SetConfigResponse) Protocol() protocol.MessageType {
	return protocol.SetConfig
}

func (r *SetConfigResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *SetConfigResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *SetConfigResponse) String() string {
	return fmt.Sprintf("SetConfigResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
