package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type SlaveConnectedResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewSlaveConnectedResponse(success bool, message string) *SlaveConnectedResponse {
	return &SlaveConnectedResponse{
		Success: success,
		Message: message,
	}
}

func (r *SlaveConnectedResponse) IsSuccess() bool {
	return r.Success
}

func (r *SlaveConnectedResponse) GetMessage() string {
	return r.Message
}

func (r *SlaveConnectedResponse) Protocol() protocol.MessageType {
	return protocol.SlaveConnected
}

func (r *SlaveConnectedResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *SlaveConnectedResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *SlaveConnectedResponse) String() string {
	return fmt.Sprintf("SlaveConnectedResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
