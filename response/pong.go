package response

import (
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type PongResponse struct{}

func NewPongResponse() *PongResponse {
	return &PongResponse{}
}

func (r *PongResponse) IsSuccess() bool {
	return true
}

func (r *PongResponse) GetMessage() string {
	return "Pong received"
}

func (r *PongResponse) Protocol() protocol.MessageType {
	return protocol.Pong
}

func (r *PongResponse) FromBytes(data []byte) error {
	return nil
}

func (r *PongResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *PongResponse) String() string {
	return "PongResponse{}"
}
