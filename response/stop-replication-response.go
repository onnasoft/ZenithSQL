package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type StopReplicationResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewStopReplicationResponse(success bool, message string) *StopReplicationResponse {
	return &StopReplicationResponse{
		Success: success,
		Message: message,
	}
}

func (r *StopReplicationResponse) IsSuccess() bool {
	return r.Success
}

func (r *StopReplicationResponse) GetMessage() string {
	return r.Message
}

func (r *StopReplicationResponse) Protocol() protocol.MessageType {
	return protocol.StopReplication
}

func (r *StopReplicationResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *StopReplicationResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *StopReplicationResponse) String() string {
	return fmt.Sprintf("StopReplicationResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
