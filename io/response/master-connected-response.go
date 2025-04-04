package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type MasterConnectedResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewMasterConnectedResponse(success bool, message string) *MasterConnectedResponse {
	return &MasterConnectedResponse{
		Success: success,
		Message: message,
	}
}

func (r *MasterConnectedResponse) IsSuccess() bool {
	return r.Success
}

func (r *MasterConnectedResponse) GetMessage() string {
	return r.Message
}

func (r *MasterConnectedResponse) Protocol() protocol.MessageType {
	return protocol.MasterConnected
}

func (r *MasterConnectedResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *MasterConnectedResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *MasterConnectedResponse) String() string {
	return fmt.Sprintf("MasterConnectedResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
