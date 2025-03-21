package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ClusterStatusResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
	Status  string `msgpack:"status"`
}

func NewClusterStatusResponse(success bool, message, status string) *ClusterStatusResponse {
	return &ClusterStatusResponse{
		Success: success,
		Message: message,
		Status:  status,
	}
}

func (r *ClusterStatusResponse) IsSuccess() bool {
	return r.Success
}

func (r *ClusterStatusResponse) GetMessage() string {
	return r.Message
}

func (r *ClusterStatusResponse) Protocol() protocol.MessageType {
	return protocol.ClusterStatus
}

func (r *ClusterStatusResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *ClusterStatusResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *ClusterStatusResponse) String() string {
	return fmt.Sprintf("ClusterStatusResponse{Success: %t, Message: %s, Status: %s}", r.Success, r.Message, r.Status)
}
