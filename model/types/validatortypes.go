package types

import (
	"fmt"
	"time"
)

// ValidatorFunc defines the signature for validation functions
type ValidatorFunc func(interface{}) error

// ValidatorTypes maps each DataType to its validation function
var ValidatorTypes = map[DataType]ValidatorFunc{
	Int8Type: func(val interface{}) error {
		if _, ok := val.(int8); !ok {
			return typeMismatchError("int8", val)
		}
		return nil
	},
	Int16Type: func(val interface{}) error {
		if _, ok := val.(int16); !ok {
			return typeMismatchError("int16", val)
		}
		return nil
	},
	Int32Type: func(val interface{}) error {
		if _, ok := val.(int32); !ok {
			return typeMismatchError("int32", val)
		}
		return nil
	},
	Int64Type: func(val interface{}) error {
		if _, ok := val.(int64); !ok {
			return typeMismatchError("int64", val)
		}
		return nil
	},
	Uint8Type: func(val interface{}) error {
		if _, ok := val.(uint8); !ok {
			return typeMismatchError("uint8", val)
		}
		return nil
	},
	Uint16Type: func(val interface{}) error {
		if _, ok := val.(uint16); !ok {
			return typeMismatchError("uint16", val)
		}
		return nil
	},
	Uint32Type: func(val interface{}) error {
		if _, ok := val.(uint32); !ok {
			return typeMismatchError("uint32", val)
		}
		return nil
	},
	Uint64Type: func(val interface{}) error {
		if _, ok := val.(uint64); !ok {
			return typeMismatchError("uint64", val)
		}
		return nil
	},
	Float32Type: func(val interface{}) error {
		if _, ok := val.(float32); !ok {
			return typeMismatchError("float32", val)
		}
		return nil
	},
	Float64Type: func(val interface{}) error {
		if _, ok := val.(float64); !ok {
			return typeMismatchError("float64", val)
		}
		return nil
	},
	StringType: func(val interface{}) error {
		if _, ok := val.(string); !ok {
			return typeMismatchError("string", val)
		}
		return nil
	},
	BoolType: func(val interface{}) error {
		if _, ok := val.(bool); !ok {
			return typeMismatchError("bool", val)
		}
		return nil
	},
	TimestampType: func(val interface{}) error {
		if val == nil {
			return nil
		}
		if _, ok := val.(time.Time); ok {
			return nil
		}
		if _, ok := val.(int64); ok {
			return nil
		}
		return fmt.Errorf("value %v is neither time.Time nor int64 type", val)
	},
}

// typeMismatchError creates a consistent type mismatch error message
func typeMismatchError(expectedType string, val interface{}) error {
	return fmt.Errorf("value %v is not of type %s", val, expectedType)
}

// ValidateValue checks if a value matches its expected data type
func ValidateValue(dataType DataType, val interface{}) error {
	validator, exists := ValidatorTypes[dataType]
	if !exists {
		return fmt.Errorf("unsupported data type: %s", dataType)
	}
	return validator(val)
}
