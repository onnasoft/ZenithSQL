package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type ReleaseSavepointStatement struct {
	TransactionID string `msgpack:"transaction_id" valid:"required,alphanumunderscore"`
	SavepointName string `msgpack:"savepoint_name" valid:"required,alphanumunderscore"`
}

func NewReleaseSavepointStatement(transactionID, savepointName string) (*ReleaseSavepointStatement, error) {
	stmt := &ReleaseSavepointStatement{
		TransactionID: transactionID,
		SavepointName: savepointName,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (r ReleaseSavepointStatement) Protocol() protocol.MessageType {
	return protocol.ReleaseSavepoint
}

func (r ReleaseSavepointStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *ReleaseSavepointStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r ReleaseSavepointStatement) String() string {
	return fmt.Sprintf("ReleaseSavepointStatement{TransactionID: %s, SavepointName: %s}", r.TransactionID, r.SavepointName)
}
