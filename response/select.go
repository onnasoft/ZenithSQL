package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type SelectResponse struct {
	Success bool                     `msgpack:"success"`
	Message string                   `msgpack:"message"`
	Rows    []map[string]interface{} `msgpack:"rows"`
}

func NewSelectResponse(success bool, message string, rows []map[string]interface{}) *SelectResponse {
	return &SelectResponse{
		Success: success,
		Message: message,
		Rows:    rows,
	}
}

func (r *SelectResponse) IsSuccess() bool {
	return r.Success
}

func (r *SelectResponse) GetMessage() string {
	return r.Message
}

func (r *SelectResponse) Protocol() protocol.MessageType {
	return protocol.Select
}

func (r *SelectResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *SelectResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *SelectResponse) String() string {
	return fmt.Sprintf("SelectResponse{Success: %t, Message: %s, Rows: %v}", r.Success, r.Message, r.Rows)
}
