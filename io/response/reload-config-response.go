package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ReloadConfigResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewReloadConfigResponse(success bool, message string) *ReloadConfigResponse {
	return &ReloadConfigResponse{
		Success: success,
		Message: message,
	}
}

func (r *ReloadConfigResponse) IsSuccess() bool {
	return r.Success
}

func (r *ReloadConfigResponse) GetMessage() string {
	return r.Message
}

func (r *ReloadConfigResponse) Protocol() protocol.MessageType {
	return protocol.ReloadConfig
}

func (r *ReloadConfigResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *ReloadConfigResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *ReloadConfigResponse) String() string {
	return fmt.Sprintf("ReloadConfigResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
