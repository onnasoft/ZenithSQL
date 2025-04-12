package aggregate

import (
	"errors"

	"github.com/onnasoft/ZenithSQL/core/buffer"
)

type Int8Aggregate struct {
	fn            AggregateType
	counted       int64
	accumulated   int64
	max           *int8
	min           *int8
	aggregateFunc func() error
	*buffer.Scanner
}

func NewInt8Aggregate(fn AggregateType, scanner *buffer.Scanner) (*Int8Aggregate, error) {
	if scanner == nil {
		return nil, errors.New("scanner cannot be nil")
	}

	agg := Int8Aggregate{
		fn:      fn,
		Scanner: scanner,
	}

	var err error
	agg.aggregateFunc, err = agg.getAggregateFunc(fn)
	if err != nil {
		return nil, err
	}

	return &agg, nil
}

func (agg *Int8Aggregate) getAggregateFunc(fn AggregateType) (func() error, error) {
	switch fn {
	case SUM, AVG:
		return agg.sumOrAvgFunc, nil
	case COUNT:
		return agg.countFunc, nil
	case MAX:
		return agg.maxFunc, nil
	case MIN:
		return agg.minFunc, nil
	case GROUP_CONCAT:
		return nil, errors.New("GROUP_CONCAT is not supported for int8")
	default:
		return nil, errors.New("unsupported aggregate function")
	}
}

func (agg *Int8Aggregate) sumOrAvgFunc() error {
	var value int8
	if ok, err := agg.Scan(&value); err != nil {
		return err
	} else if ok {
		agg.accumulated += int64(value)
		agg.counted++
	}
	return nil
}

func (agg *Int8Aggregate) countFunc() error {
	var value int8
	if ok, err := agg.Scan(&value); err != nil {
		return err
	} else if ok {
		agg.counted++
	}
	return nil
}

func (agg *Int8Aggregate) maxFunc() error {
	var value int8
	if ok, err := agg.Scan(&value); err != nil {
		return err
	} else if ok {
		if agg.max == nil || value > *agg.max {
			agg.max = &value
		}
	}
	return nil
}

func (agg *Int8Aggregate) minFunc() error {
	var value int8
	if ok, err := agg.Scan(&value); err != nil {
		return err
	} else if ok {
		if agg.min == nil || value < *agg.min {
			agg.min = &value
		}
	}
	return nil
}

func (agg *Int8Aggregate) Result() (interface{}, error) {
	switch agg.fn {
	case SUM:
		return float64(agg.accumulated), nil
	case AVG:
		if agg.counted == 0 {
			return nil, errors.New("division by zero")
		}
		return float64(agg.accumulated) / float64(agg.counted), nil
	case COUNT:
		return float64(agg.counted), nil
	case MAX:
		if agg.max == nil {
			return nil, nil
		}
		return float64(*agg.max), nil
	case MIN:
		if agg.min == nil {
			return nil, nil
		}
		return float64(*agg.min), nil
	default:
		return nil, errors.New("unsupported aggregate function")
	}
}

func (agg *Int8Aggregate) Execute() error {
	if agg.aggregateFunc == nil {
		return errors.New("aggregate function not set")
	}
	return agg.aggregateFunc()
}

func (agg *Int8Aggregate) Reset() error {
	agg.counted = 0
	agg.accumulated = 0
	agg.max = nil
	agg.min = nil
	return nil
}
