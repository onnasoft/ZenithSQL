package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type GreetingResponse struct {
	Message string `msgpack:"message"`
}

func NewGreetingResponse(message string) *GreetingResponse {
	return &GreetingResponse{Message: message}
}

func (r *GreetingResponse) IsSuccess() bool {
	return true
}

func (r *GreetingResponse) GetMessage() string {
	return r.Message
}

func (r *GreetingResponse) Protocol() protocol.MessageType {
	return protocol.Greeting
}

func (r *GreetingResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *GreetingResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *GreetingResponse) String() string {
	return fmt.Sprintf("GreetingResponse{Message: %s}", r.Message)
}
