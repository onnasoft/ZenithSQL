package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type DescribeTableResponse struct {
	Success bool                   `msgpack:"success"`
	Message string                 `msgpack:"message"`
	Columns map[string]interface{} `msgpack:"columns"`
}

func NewDescribeTableResponse(success bool, message string, columns map[string]interface{}) *DescribeTableResponse {
	return &DescribeTableResponse{
		Success: success,
		Message: message,
		Columns: columns,
	}
}

func (r *DescribeTableResponse) IsSuccess() bool {
	return r.Success
}

func (r *DescribeTableResponse) GetMessage() string {
	return r.Message
}

func (r *DescribeTableResponse) Protocol() protocol.MessageType {
	return protocol.DescribeTable
}

func (r *DescribeTableResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *DescribeTableResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *DescribeTableResponse) String() string {
	return fmt.Sprintf("DescribeTableResponse{Success: %t, Message: %s, Columns: %v}", r.Success, r.Message, r.Columns)
}
