package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type WelcomeResponse struct {
	Message string `msgpack:"message"`
}

func NewWelcomeResponse(message string) *WelcomeResponse {
	return &WelcomeResponse{Message: message}
}

func (r *WelcomeResponse) IsSuccess() bool {
	return true
}

func (r *WelcomeResponse) GetMessage() string {
	return r.Message
}

func (r *WelcomeResponse) Protocol() protocol.MessageType {
	return protocol.Welcome
}

func (r *WelcomeResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *WelcomeResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *WelcomeResponse) String() string {
	return fmt.Sprintf("WelcomeResponse{Message: %s}", r.Message)
}
