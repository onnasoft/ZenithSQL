package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CommitStatement struct {
	TransactionID string `msgpack:"transaction_id" valid:"required,alphanumunderscore"`
}

func NewCommitStatement(transactionID string) (*CommitStatement, error) {
	stmt := &CommitStatement{
		TransactionID: transactionID,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (c CommitStatement) Protocol() protocol.MessageType {
	return protocol.Commit
}

func (c CommitStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(c)
}

func (c *CommitStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, c)
}

func (c CommitStatement) String() string {
	return fmt.Sprintf("CommitStatement{TransactionID: %s}", c.TransactionID)
}
