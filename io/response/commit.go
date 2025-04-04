package response

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CommitResponse struct {
	Success bool   `msgpack:"success"`
	Message string `msgpack:"message"`
}

func NewCommitResponse(success bool, message string) *CommitResponse {
	return &CommitResponse{
		Success: success,
		Message: message,
	}
}

func (r *CommitResponse) IsSuccess() bool {
	return r.Success
}

func (r *CommitResponse) GetMessage() string {
	return r.Message
}

func (r *CommitResponse) Protocol() protocol.MessageType {
	return protocol.Commit
}

func (r *CommitResponse) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *CommitResponse) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *CommitResponse) String() string {
	return fmt.Sprintf("CommitResponse{Success: %t, Message: %s}", r.Success, r.Message)
}
