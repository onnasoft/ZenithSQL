package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type InsertResponse struct {
	Success      bool          `msgpack:"success"`
	Message      string        `msgpack:"message"`
	InsertedIDs  []interface{} `msgpack:"inserted_ids"`
	RowsAffected int64         `msgpack:"rows_affected"`
	DurationMs   int64         `msgpack:"duration_ms"`
}

func NewInsertResponse(success bool, message string, insertedIDs []interface{}, rowsAffected int64, durationMs int64) *InsertResponse {
	return &InsertResponse{
		Success:      success,
		Message:      message,
		InsertedIDs:  insertedIDs,
		RowsAffected: rowsAffected,
		DurationMs:   durationMs,
	}
}

func (r *InsertResponse) IsSuccess() bool {
	return r.Success
}

func (r *InsertResponse) GetMessage() string {
	return r.Message
}

func (r *InsertResponse) GetInsertedIDs() []interface{} {
	return r.InsertedIDs
}

func (r *InsertResponse) GetRowsAffected() int64 {
	return r.RowsAffected
}

func (r *InsertResponse) GetDurationMs() int64 {
	return r.DurationMs
}

func (r *InsertResponse) Protocol() protocol.MessageType {
	return protocol.Insert
}

func (r *InsertResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *InsertResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *InsertResponse) String() string {
	return fmt.Sprintf(
		"InsertResponse{Success: %t, Rows: %d, Duration: %dms, IDs: %v, Message: %s}",
		r.Success,
		r.RowsAffected,
		r.DurationMs,
		r.InsertedIDs,
		r.Message,
	)
}
