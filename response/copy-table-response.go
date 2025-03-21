package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CopyTableResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewCopyTableResponse(success bool, message string) *CopyTableResponse {
	return &CopyTableResponse{
		Success: success,
		Message: message,
	}
}

func (r *CopyTableResponse) IsSuccess() bool {
	return r.Success
}

func (r *CopyTableResponse) GetMessage() string {
	return r.Message
}

func (r *CopyTableResponse) Protocol() protocol.MessageType {
	return protocol.CopyTable
}

func (r *CopyTableResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *CopyTableResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *CopyTableResponse) String() string {
	return fmt.Sprintf("CopyTableResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
