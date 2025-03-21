package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type DemoteToSlaveResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewDemoteToSlaveResponse(success bool, message string) *DemoteToSlaveResponse {
	return &DemoteToSlaveResponse{
		Success: success,
		Message: message,
	}
}

func (r *DemoteToSlaveResponse) IsSuccess() bool {
	return r.Success
}

func (r *DemoteToSlaveResponse) GetMessage() string {
	return r.Message
}

func (r *DemoteToSlaveResponse) Protocol() protocol.MessageType {
	return protocol.DemoteToSlave
}

func (r *DemoteToSlaveResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *DemoteToSlaveResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *DemoteToSlaveResponse) String() string {
	return fmt.Sprintf("DemoteToSlaveResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
