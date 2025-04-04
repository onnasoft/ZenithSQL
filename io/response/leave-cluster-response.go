package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type LeaveClusterResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewLeaveClusterResponse(success bool, message string) *LeaveClusterResponse {
	return &LeaveClusterResponse{
		Success: success,
		Message: message,
	}
}

func (r *LeaveClusterResponse) IsSuccess() bool {
	return r.Success
}

func (r *LeaveClusterResponse) GetMessage() string {
	return r.Message
}

func (r *LeaveClusterResponse) Protocol() protocol.MessageType {
	return protocol.LeaveCluster
}

func (r *LeaveClusterResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *LeaveClusterResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *LeaveClusterResponse) String() string {
	return fmt.Sprintf("LeaveClusterResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
