package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type RenameTableStatement struct {
	OldTableName string `msgpack:"old_table_name" valid:"required,alphanumunderscore"`
	NewTableName string `msgpack:"new_table_name" valid:"required,alphanumunderscore"`
}

func NewRenameTableStatement(oldName, newName string) (*RenameTableStatement, error) {
	stmt := &RenameTableStatement{OldTableName: oldName, NewTableName: newName}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (r RenameTableStatement) Protocol() protocol.MessageType {
	return protocol.RenameTable
}

func (r RenameTableStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

func (r *RenameTableStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r RenameTableStatement) String() string {
	return fmt.Sprintf("RenameTableStatement{OldTableName: %s, NewTableName: %s}", r.OldTableName, r.NewTableName)
}
