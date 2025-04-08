package response

import (
	"errors"
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

var ErrUnsupportedResponse = errors.New("unsupported response type")

func NewErrUnsupportedResponse() error {
	return ErrUnsupportedResponse
}

type ErrorResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewErrorResponse(message string) *ErrorResponse {
	return &ErrorResponse{
		Success: false,
		Message: message,
	}
}

func (r *ErrorResponse) IsSuccess() bool {
	return r.Success
}

func (r *ErrorResponse) GetMessage() string {
	return r.Message
}

func (r *ErrorResponse) Protocol() protocol.MessageType {
	return protocol.Update
}

func (r *ErrorResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *ErrorResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *ErrorResponse) String() string {
	return fmt.Sprintf("ErrorResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
