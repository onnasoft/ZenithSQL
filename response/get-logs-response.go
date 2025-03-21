package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type GetLogsResponse struct {
	Success bool          `msgpack:"success"`
	Message string        `msgpack:"message"`
	Logs    []interface{} `msgpack:"logs"`
}

func NewGetLogsResponse(success bool, message string, logs []interface{}) *GetLogsResponse {
	return &GetLogsResponse{
		Success: success,
		Message: message,
		Logs:    logs,
	}
}

func (r *GetLogsResponse) IsSuccess() bool {
	return r.Success
}

func (r *GetLogsResponse) GetMessage() string {
	return r.Message
}

func (r *GetLogsResponse) Protocol() protocol.MessageType {
	return protocol.GetLogs
}

func (r *GetLogsResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *GetLogsResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *GetLogsResponse) String() string {
	return fmt.Sprintf("GetLogsResponse{Success: %t, Message: %s, Logs: %v}", r.Success, r.Message, r.Logs)
}
