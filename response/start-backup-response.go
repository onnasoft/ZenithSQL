package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type StartBackupResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewStartBackupResponse(success bool, message string) *StartBackupResponse {
	return &StartBackupResponse{
		Success: success,
		Message: message,
	}
}

func (r *StartBackupResponse) IsSuccess() bool {
	return r.Success
}

func (r *StartBackupResponse) GetMessage() string {
	return r.Message
}

func (r *StartBackupResponse) Protocol() protocol.MessageType {
	return protocol.StartBackup
}

func (r *StartBackupResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *StartBackupResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *StartBackupResponse) String() string {
	return fmt.Sprintf("StartBackupResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
