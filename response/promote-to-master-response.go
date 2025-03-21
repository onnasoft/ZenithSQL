package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type PromoteToMasterResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewPromoteToMasterResponse(success bool, message string) *PromoteToMasterResponse {
	return &PromoteToMasterResponse{
		Success: success,
		Message: message,
	}
}

func (r *PromoteToMasterResponse) IsSuccess() bool {
	return r.Success
}

func (r *PromoteToMasterResponse) GetMessage() string {
	return r.Message
}

func (r *PromoteToMasterResponse) Protocol() protocol.MessageType {
	return protocol.PromoteToMaster
}

func (r *PromoteToMasterResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *PromoteToMasterResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *PromoteToMasterResponse) String() string {
	return fmt.Sprintf("PromoteToMasterResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
