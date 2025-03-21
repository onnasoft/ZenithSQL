package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type SyncDataResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewSyncDataResponse(success bool, message string) *SyncDataResponse {
	return &SyncDataResponse{
		Success: success,
		Message: message,
	}
}

func (r *SyncDataResponse) IsSuccess() bool {
	return r.Success
}

func (r *SyncDataResponse) GetMessage() string {
	return r.Message
}

func (r *SyncDataResponse) Protocol() protocol.MessageType {
	return protocol.SyncData
}

func (r *SyncDataResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *SyncDataResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *SyncDataResponse) String() string {
	return fmt.Sprintf("SyncDataResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
