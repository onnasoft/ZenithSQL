package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ShowDatabasesResponse struct {
	Success   bool     `msgpack:"success"`
	Message   string   `msgpack:"message"`
	Databases []string `msgpack:"databases"`
}

func NewShowDatabasesResponse(success bool, message string, databases []string) *ShowDatabasesResponse {
	return &ShowDatabasesResponse{
		Success:   success,
		Message:   message,
		Databases: databases,
	}
}

func (r *ShowDatabasesResponse) IsSuccess() bool {
	return r.Success
}

func (r *ShowDatabasesResponse) GetMessage() string {
	return r.Message
}

func (r *ShowDatabasesResponse) Protocol() protocol.MessageType {
	return protocol.ShowDatabases
}

func (r *ShowDatabasesResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *ShowDatabasesResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *ShowDatabasesResponse) String() string {
	return fmt.Sprintf("ShowDatabasesResponse{Success: %t, Message: %s, Databases: %v}", r.Success, r.Message, r.Databases)
}
