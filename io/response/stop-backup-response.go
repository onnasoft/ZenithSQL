package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type StopBackupResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewStopBackupResponse(success bool, message string) *StopBackupResponse {
	return &StopBackupResponse{
		Success: success,
		Message: message,
	}
}

func (r *StopBackupResponse) IsSuccess() bool {
	return r.Success
}

func (r *StopBackupResponse) GetMessage() string {
	return r.Message
}

func (r *StopBackupResponse) Protocol() protocol.MessageType {
	return protocol.StopBackup
}

func (r *StopBackupResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *StopBackupResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *StopBackupResponse) String() string {
	return fmt.Sprintf("StopBackupResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
