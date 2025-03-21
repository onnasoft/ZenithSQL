package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ReplicationStatusResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
	Status  string `msgpack:"status"`
}

func NewReplicationStatusResponse(success bool, message, status string) *ReplicationStatusResponse {
	return &ReplicationStatusResponse{
		Success: success,
		Message: message,
		Status:  status,
	}
}

func (r *ReplicationStatusResponse) IsSuccess() bool {
	return r.Success
}

func (r *ReplicationStatusResponse) GetMessage() string {
	return r.Message
}

func (r *ReplicationStatusResponse) Protocol() protocol.MessageType {
	return protocol.ReplicationStatus
}

func (r *ReplicationStatusResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *ReplicationStatusResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *ReplicationStatusResponse) String() string {
	return fmt.Sprintf("ReplicationStatusResponse{Success: %t, Message: %s, Status: %s}", r.Success, r.Message, r.Status)
}
