package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type BulkInsertResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewBulkInsertResponse(success bool, message string) *BulkInsertResponse {
	return &BulkInsertResponse{
		Success: success,
		Message: message,
	}
}

func (r *BulkInsertResponse) IsSuccess() bool {
	return r.Success
}

func (r *BulkInsertResponse) GetMessage() string {
	return r.Message
}

func (r *BulkInsertResponse) Protocol() protocol.MessageType {
	return protocol.BulkInsert
}

func (r *BulkInsertResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *BulkInsertResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *BulkInsertResponse) String() string {
	return fmt.Sprintf("BulkInsertResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
