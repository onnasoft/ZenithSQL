package aggregate

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/core/buffer"
	"github.com/onnasoft/ZenithSQL/model/fields"
)

type AggregateType string

const (
	SUM          AggregateType = "SUM"
	AVG          AggregateType = "AVG"
	COUNT        AggregateType = "COUNT"
	MAX          AggregateType = "MAX"
	MIN          AggregateType = "MIN"
	GROUP_CONCAT AggregateType = "GROUP_CONCAT"
)

type Aggregate interface {
	Result() (interface{}, error)
	Execute() error
	Reset() error
}

func New(dataType fields.DataType, fn AggregateType, scanner *buffer.Scanner) (Aggregate, error) {
	switch dataType.String() {
	case "int8":
		return NewInt8Aggregate(fn, scanner)
	}

	return nil, fmt.Errorf("unsupported data type: %s", dataType.String())
}
