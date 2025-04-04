package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type DescribeTableStatement struct {
	TableName string `msgpack:"table_name" valid:"required,alphanumunderscore"`
}

func NewDescribeTableStatement(tableName string) (*DescribeTableStatement, error) {
	stmt := &DescribeTableStatement{TableName: tableName}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (d DescribeTableStatement) Protocol() protocol.MessageType {
	return protocol.DescribeTable
}

func (d DescribeTableStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(d)
}

func (d *DescribeTableStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, d)
}

func (d DescribeTableStatement) String() string {
	return fmt.Sprintf("DescribeTableStatement{TableName: %s}", d.TableName)
}
