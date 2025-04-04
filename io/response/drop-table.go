package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type DropTableResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewDropTableResponse(success bool, message string) *DropTableResponse {
	return &DropTableResponse{
		Success: success,
		Message: message,
	}
}

func (r *DropTableResponse) IsSuccess() bool {
	return r.Success
}

func (r *DropTableResponse) GetMessage() string {
	return r.Message
}

func (r *DropTableResponse) Protocol() protocol.MessageType {
	return protocol.DropTable
}

func (r *DropTableResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *DropTableResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *DropTableResponse) String() string {
	return fmt.Sprintf("DropTableResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
