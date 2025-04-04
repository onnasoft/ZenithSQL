package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type RollbackStatement struct {
	TransactionID string `msgpack:"transaction_id" valid:"required,alphanumunderscore"`
}

func NewRollbackStatement(transactionID string) (*RollbackStatement, error) {
	stmt := &RollbackStatement{
		TransactionID: transactionID,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (r RollbackStatement) Protocol() protocol.MessageType {
	return protocol.Rollback
}

func (r RollbackStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *RollbackStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r RollbackStatement) String() string {
	return fmt.Sprintf("RollbackStatement{TransactionID: %s}", r.TransactionID)
}
