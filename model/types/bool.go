package types

import "errors"

const errTypeAssertionBoolFailed = "type assertion failed for Bool"

type BoolType struct{}

func (BoolType) ResolveLength(length int) (int, error) {
	return 1, nil
}

func (BoolType) Read(data []byte, out interface{}) error {
	v, ok := out.(*bool)
	if !ok {
		return errors.New(errTypeAssertionBoolFailed)
	}
	*v = data[0] != 0
	return nil
}

func (BoolType) Write(buffer []byte, value interface{}) error {
	v, ok := value.(bool)
	if !ok && value != nil {
		return errors.New(errTypeAssertionBoolFailed)
	}
	if v {
		buffer[0] = 1
	} else {
		buffer[0] = 0
	}
	return nil
}

func (BoolType) Valid(value interface{}) error {
	_, ok := value.(bool)
	if !ok && value != nil {
		return errors.New(errTypeAssertionBoolFailed)
	}
	return nil
}

func (BoolType) Parse(data []byte) interface{} {
	v := data[0] != 0
	return v
}

func (BoolType) String() string {
	return "bool"
}
