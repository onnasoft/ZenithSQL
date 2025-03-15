package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type UpsertResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewUpsertResponse(success bool, message string) *UpsertResponse {
	return &UpsertResponse{
		Success: success,
		Message: message,
	}
}

func (r *UpsertResponse) IsSuccess() bool {
	return r.Success
}

func (r *UpsertResponse) GetMessage() string {
	return r.Message
}

func (r *UpsertResponse) Protocol() protocol.MessageType {
	return protocol.Upsert
}

func (r *UpsertResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *UpsertResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *UpsertResponse) String() string {
	return fmt.Sprintf("UpsertResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
