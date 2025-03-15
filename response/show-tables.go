package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ShowTablesResponse struct {
	Success bool     `msgpack:"success"`
	Message string   `msgpack:"message"`
	Tables  []string `msgpack:"tables"`
}

func NewShowTablesResponse(success bool, message string, tables []string) *ShowTablesResponse {
	return &ShowTablesResponse{
		Success: success,
		Message: message,
		Tables:  tables,
	}
}

func (r *ShowTablesResponse) IsSuccess() bool {
	return r.Success
}

func (r *ShowTablesResponse) GetMessage() string {
	return r.Message
}

func (r *ShowTablesResponse) Protocol() protocol.MessageType {
	return protocol.ShowTables
}

func (r *ShowTablesResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *ShowTablesResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *ShowTablesResponse) String() string {
	return fmt.Sprintf("ShowTablesResponse{Success: %t, Message: %s, Tables: %v}", r.Success, r.Message, r.Tables)
}
