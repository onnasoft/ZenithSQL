package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CustomCommandResponse struct {
	Success bool        `msgpack:"success"`
	Message string      `msgpack:"message"`
	Data    interface{} `msgpack:"data"`
}

func NewCustomCommandResponse(success bool, message string, data interface{}) *CustomCommandResponse {
	return &CustomCommandResponse{
		Success: success,
		Message: message,
		Data:    data,
	}
}

func (r *CustomCommandResponse) IsSuccess() bool {
	return r.Success
}

func (r *CustomCommandResponse) GetMessage() string {
	return r.Message
}

func (r *CustomCommandResponse) Protocol() protocol.MessageType {
	return protocol.CustomCommand
}

func (r *CustomCommandResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *CustomCommandResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *CustomCommandResponse) String() string {
	return fmt.Sprintf("CustomCommandResponse{Success: %t, Message: %s, Data: %v}", r.Success, r.Message, r.Data)
}
