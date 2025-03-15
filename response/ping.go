package response

import (
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type PingResponse struct{}

func NewPingResponse() *PingResponse {
	return &PingResponse{}
}

func (r *PingResponse) IsSuccess() bool {
	return true
}

func (r *PingResponse) GetMessage() string {
	return "Ping received"
}

func (r *PingResponse) Protocol() protocol.MessageType {
	return protocol.Ping
}

func (r *PingResponse) FromBytes(data []byte) error {
	return nil
}

func (r *PingResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *PingResponse) String() string {
	return "PingResponse{}"
}
