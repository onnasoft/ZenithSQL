package statement

import (
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type QueryStatement struct {
	Query string `msgpack:"query" valid:"required"`
}

func NewQueryStatement(query string) (*QueryStatement, error) {
	stmt := &QueryStatement{
		Query: query,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (q QueryStatement) Protocol() protocol.MessageType {
	return protocol.Query
}

func (q QueryStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(q)
}

func (q *QueryStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, q)
}

func (q QueryStatement) String() string {
	return fmt.Sprintf("QueryStatement{Query: %s}", q.Query)
}
