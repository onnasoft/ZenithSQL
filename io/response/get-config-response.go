package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type GetConfigResponse struct {
	Success bool        `msgpack:"success"`
	Message string      `msgpack:"message"`
	Config  interface{} `msgpack:"config"`
}

func NewGetConfigResponse(success bool, message string, config interface{}) *GetConfigResponse {
	return &GetConfigResponse{
		Success: success,
		Message: message,
		Config:  config,
	}
}

func (r *GetConfigResponse) IsSuccess() bool {
	return r.Success
}

func (r *GetConfigResponse) GetMessage() string {
	return r.Message
}

func (r *GetConfigResponse) Protocol() protocol.MessageType {
	return protocol.GetConfig
}

func (r *GetConfigResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *GetConfigResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *GetConfigResponse) String() string {
	return fmt.Sprintf("GetConfigResponse{Success: %t, Message: %s, Config: %v}", r.Success, r.Message, r.Config)
}
