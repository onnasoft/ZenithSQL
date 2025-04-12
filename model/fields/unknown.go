package fields

import "errors"

type UnknownType struct{}

func (UnknownType) ResolveLength(length int) (int, error) {
	return 0, nil
}

func (UnknownType) Read(data []byte, out interface{}) error {
	if len(data) == 0 {
		return nil
	}
	ptr, ok := out.(*interface{})
	if !ok {
		return errors.New("output must be *interface{}")
	}
	*ptr = data
	return nil
}

func (UnknownType) Write(buffer []byte, value interface{}) error {
	v, ok := value.([]byte)
	if !ok && value != nil {
		return errors.New("type assertion failed for Unknown")
	}
	copy(buffer, v)
	return nil
}

func (UnknownType) Parse(data []byte) interface{} {
	if len(data) == 0 {
		return nil
	}
	return data
}

func (UnknownType) Valid(val interface{}) error {
	if _, ok := val.([]byte); !ok && val != nil {
		return errors.New("value is not of type []byte")
	}
	return nil
}

func (UnknownType) String() string {
	return "unknown"
}
