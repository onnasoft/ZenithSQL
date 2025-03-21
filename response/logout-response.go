package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type LogoutResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewLogoutResponse(success bool, message string) *LogoutResponse {
	return &LogoutResponse{
		Success: success,
		Message: message,
	}
}

func (r *LogoutResponse) IsSuccess() bool {
	return r.Success
}

func (r *LogoutResponse) GetMessage() string {
	return r.Message
}

func (r *LogoutResponse) Protocol() protocol.MessageType {
	return protocol.Logout
}

func (r *LogoutResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *LogoutResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *LogoutResponse) String() string {
	return fmt.Sprintf("LogoutResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
