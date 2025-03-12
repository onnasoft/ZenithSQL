package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type SavepointStatement struct {
	TransactionID string `msgpack:"transaction_id" valid:"required,alphanumunderscore"`
	SavepointName string `msgpack:"savepoint_name" valid:"required,alphanumunderscore"`
}

func NewSavepointStatement(transactionID, savepointName string) (*SavepointStatement, error) {
	stmt := &SavepointStatement{
		TransactionID: transactionID,
		SavepointName: savepointName,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (s SavepointStatement) Protocol() protocol.MessageType {
	return protocol.Savepoint
}

func (s SavepointStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(s)
}

func (s *SavepointStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, s)
}

func (s SavepointStatement) String() string {
	return fmt.Sprintf("SavepointStatement{TransactionID: %s, SavepointName: %s}", s.TransactionID, s.SavepointName)
}
