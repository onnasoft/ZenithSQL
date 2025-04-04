package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type BeginTransactionStatement struct {
	TransactionID string `msgpack:"transaction_id" valid:"required,alphanumunderscore"`
}

func NewBeginTransactionStatement(transactionID string) (*BeginTransactionStatement, error) {
	stmt := &BeginTransactionStatement{
		TransactionID: transactionID,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (b BeginTransactionStatement) Protocol() protocol.MessageType {
	return protocol.BeginTransaction
}

func (b BeginTransactionStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(b)
}

func (b *BeginTransactionStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, b)
}

func (b BeginTransactionStatement) String() string {
	return fmt.Sprintf("BeginTransactionStatement{TransactionID: %s}", b.TransactionID)
}
