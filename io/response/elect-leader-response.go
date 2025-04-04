package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ElectLeaderResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewElectLeaderResponse(success bool, message string) *ElectLeaderResponse {
	return &ElectLeaderResponse{
		Success: success,
		Message: message,
	}
}

func (r *ElectLeaderResponse) IsSuccess() bool {
	return r.Success
}

func (r *ElectLeaderResponse) GetMessage() string {
	return r.Message
}

func (r *ElectLeaderResponse) Protocol() protocol.MessageType {
	return protocol.ElectLeader
}

func (r *ElectLeaderResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *ElectLeaderResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *ElectLeaderResponse) String() string {
	return fmt.Sprintf("ElectLeaderResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
