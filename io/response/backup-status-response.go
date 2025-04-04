package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type BackupStatusResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
	Status  string `msgpack:"status"`
}

func NewBackupStatusResponse(success bool, message, status string) *BackupStatusResponse {
	return &BackupStatusResponse{
		Success: success,
		Message: message,
		Status:  status,
	}
}

func (r *BackupStatusResponse) IsSuccess() bool {
	return r.Success
}

func (r *BackupStatusResponse) GetMessage() string {
	return r.Message
}

func (r *BackupStatusResponse) Protocol() protocol.MessageType {
	return protocol.BackupStatus
}

func (r *BackupStatusResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *BackupStatusResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *BackupStatusResponse) String() string {
	return fmt.Sprintf("BackupStatusResponse{Success: %t, Message: %s, Status: %s}", r.Success, r.Message, r.Status)
}
