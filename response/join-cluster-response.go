package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type JoinClusterResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewJoinClusterResponse(success bool, message string) *JoinClusterResponse {
	return &JoinClusterResponse{
		Success: success,
		Message: message,
	}
}

func (r *JoinClusterResponse) IsSuccess() bool {
	return r.Success
}

func (r *JoinClusterResponse) GetMessage() string {
	return r.Message
}

func (r *JoinClusterResponse) Protocol() protocol.MessageType {
	return protocol.JoinCluster
}

func (r *JoinClusterResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *JoinClusterResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *JoinClusterResponse) String() string {
	return fmt.Sprintf("JoinClusterResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
