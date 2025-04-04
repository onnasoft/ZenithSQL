package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type RenameTableResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewRenameTableResponse(success bool, message string) *RenameTableResponse {
	return &RenameTableResponse{
		Success: success,
		Message: message,
	}
}

func (r *RenameTableResponse) IsSuccess() bool {
	return r.Success
}

func (r *RenameTableResponse) GetMessage() string {
	return r.Message
}

func (r *RenameTableResponse) Protocol() protocol.MessageType {
	return protocol.RenameTable
}

func (r *RenameTableResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *RenameTableResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *RenameTableResponse) String() string {
	return fmt.Sprintf("RenameTableResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
