package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type AlterTableResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewAlterTableResponse(success bool, message string) *AlterTableResponse {
	return &AlterTableResponse{
		Success: success,
		Message: message,
	}
}

func (r *AlterTableResponse) IsSuccess() bool {
	return r.Success
}

func (r *AlterTableResponse) GetMessage() string {
	return r.Message
}

func (r *AlterTableResponse) Protocol() protocol.MessageType {
	return protocol.AlterTable
}

func (r *AlterTableResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *AlterTableResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *AlterTableResponse) String() string {
	return fmt.Sprintf("AlterTableResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
