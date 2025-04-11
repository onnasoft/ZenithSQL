package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

// ImportResponse representa la respuesta a una operación IMPORT
type ImportResponse struct {
	Success      bool   `msgpack:"success"`
	Message      string `msgpack:"message"`
	RowsImported int64  `msgpack:"rows_imported"`
	DurationMs   int64  `msgpack:"duration_ms"`
}

// NewImportResponse crea una nueva respuesta de importación
func NewImportResponse(success bool, message string, rowsImported, durationMs int64) *ImportResponse {
	return &ImportResponse{
		Success:      success,
		Message:      message,
		RowsImported: rowsImported,
		DurationMs:   durationMs,
	}
}

// IsSuccess indica si la operación fue exitosa
func (r *ImportResponse) IsSuccess() bool {
	return r.Success
}

// GetMessage devuelve el mensaje descriptivo
func (r *ImportResponse) GetMessage() string {
	return r.Message
}

// GetRowsImported devuelve el número de filas importadas
func (r *ImportResponse) GetRowsImported() int64 {
	return r.RowsImported
}

// GetDurationMs devuelve el tiempo de ejecución en milisegundos
func (r *ImportResponse) GetDurationMs() int64 {
	return r.DurationMs
}

// Protocol implementa la interfaz Message
func (r *ImportResponse) Protocol() protocol.MessageType {
	return protocol.Import
}

// FromBytes deserializa desde MessagePack
func (r *ImportResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

// ToBytes serializa a MessagePack
func (r *ImportResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

// String representa la respuesta como string
func (r *ImportResponse) String() string {
	return fmt.Sprintf(
		"ImportResponse{Success: %t, Rows: %d, Duration: %dms, Message: %s}",
		r.Success,
		r.RowsImported,
		r.DurationMs,
		r.Message,
	)
}
