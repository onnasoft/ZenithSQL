package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type QueryResponse struct {
	Success bool        `msgpack:"success"`
	Message string      `msgpack:"message"`
	Data    interface{} `msgpack:"data"`
}

func NewQueryResponse(success bool, message string, data interface{}) *QueryResponse {
	return &QueryResponse{
		Success: success,
		Message: message,
		Data:    data,
	}
}

func (r *QueryResponse) IsSuccess() bool {
	return r.Success
}

func (r *QueryResponse) GetMessage() string {
	return r.Message
}

func (r *QueryResponse) Protocol() protocol.MessageType {
	return protocol.Query
}

func (r *QueryResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *QueryResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *QueryResponse) String() string {
	return fmt.Sprintf("QueryResponse{Success: %t, Message: %s, Data: %v}", r.Success, r.Message, r.Data)
}
