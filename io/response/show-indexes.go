package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ShowIndexesResponse struct {
	Success bool     `msgpack:"success"`
	Message string   `msgpack:"message"`
	Indexes []string `msgpack:"indexes"`
}

func NewShowIndexesResponse(success bool, message string, indexes []string) *ShowIndexesResponse {
	return &ShowIndexesResponse{
		Success: success,
		Message: message,
		Indexes: indexes,
	}
}

func (r *ShowIndexesResponse) IsSuccess() bool {
	return r.Success
}

func (r *ShowIndexesResponse) GetMessage() string {
	return r.Message
}

func (r *ShowIndexesResponse) Protocol() protocol.MessageType {
	return protocol.ShowIndexes
}

func (r *ShowIndexesResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *ShowIndexesResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *ShowIndexesResponse) String() string {
	return fmt.Sprintf("ShowIndexesResponse{Success: %t, Message: %s, Indexes: %v}", r.Success, r.Message, r.Indexes)
}
