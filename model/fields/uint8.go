package fields

import "errors"

type Uint8Type struct{}

func (Uint8Type) ResolveLength(length int) (int, error) {
	return 1, nil
}

func (Uint8Type) Read(data []byte, out interface{}) error {
	if len(data) < 1 {
		return errors.New("data too short")
	}
	if v, ok := out.(*uint8); ok {
		*v = uint8(data[0])
		return nil
	}
	return errors.New("output type mismatch")
}

func (Uint8Type) Write(buffer []byte, value interface{}) error {
	if len(buffer) < 1 {
		return errors.New("buffer too short")
	}
	if v, ok := value.(uint8); ok {
		buffer[0] = byte(v)
		return nil
	}
	return errors.New("value type mismatch")
}

func (Uint8Type) Parse(data []byte) interface{} {
	if len(data) < 1 {
		return nil
	}
	return uint8(data[0])
}

func (Uint8Type) Valid(value interface{}) error {
	if _, ok := value.(uint8); !ok {
		return errors.New("value is not uint8")
	}
	return nil
}

func (Uint8Type) String() string {
	return "uint8"
}
