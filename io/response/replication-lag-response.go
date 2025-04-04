package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ReplicationLagResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
	Lag     int64  `msgpack:"lag"`
}

func NewReplicationLagResponse(success bool, message string, lag int64) *ReplicationLagResponse {
	return &ReplicationLagResponse{
		Success: success,
		Message: message,
		Lag:     lag,
	}
}

func (r *ReplicationLagResponse) IsSuccess() bool {
	return r.Success
}

func (r *ReplicationLagResponse) GetMessage() string {
	return r.Message
}

func (r *ReplicationLagResponse) Protocol() protocol.MessageType {
	return protocol.ReplicationLag
}

func (r *ReplicationLagResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *ReplicationLagResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *ReplicationLagResponse) String() string {
	return fmt.Sprintf("ReplicationLagResponse{Success: %t, Message: %s, Lag: %d}", r.Success, r.Message, r.Lag)
}
