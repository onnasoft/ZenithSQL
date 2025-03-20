package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type CopyTableStatement struct {
	SourceTable      string `msgpack:"source_table" valid:"required,alphanumunderscore"`
	DestinationTable string `msgpack:"destination_table" valid:"required,alphanumunderscore"`
}

func NewCopyTableStatement(sourceTable, destinationTable string) (*CopyTableStatement, error) {
	stmt := &CopyTableStatement{
		SourceTable:      sourceTable,
		DestinationTable: destinationTable,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (c CopyTableStatement) Protocol() protocol.MessageType {
	return protocol.CopyTable
}

func (c CopyTableStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(c)
}

func (c *CopyTableStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, c)
}

func (c CopyTableStatement) String() string {
	return fmt.Sprintf("CopyTableStatement{SourceTable: %s, DestinationTable: %s}", c.SourceTable, c.DestinationTable)
}
