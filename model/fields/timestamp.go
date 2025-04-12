package fields

import (
	"errors"
	"fmt"
	"time"
	"unsafe"
)

type TimestampType struct{}

func (TimestampType) ResolveLength(length int) (int, error) {
	return 8, nil
}

func (TimestampType) Read(data []byte, out interface{}) error {
	if len(data) < 8 {
		return errors.New("insufficient data for Timestamp (need 8 bytes)")
	}
	ptr, ok := out.(*time.Time)
	if !ok {
		return errors.New("output must be *time.Time")
	}
	*ptr = time.Unix(0, *(*int64)(unsafe.Pointer(&data[0])))
	return nil
}

func (TimestampType) Write(buffer []byte, value interface{}) error {
	v, ok := value.(time.Time)
	if !ok && value != nil {
		return fmt.Errorf("type assertion failed for Timestamp")
	}
	*(*int64)(unsafe.Pointer(&buffer[0])) = v.UnixNano()
	return nil
}

func (TimestampType) Valid(value interface{}) error {
	if value == nil {
		return nil
	}
	if _, ok := value.(time.Time); ok {
		return nil
	}
	if _, ok := value.(int64); ok {
		return nil
	}
	return fmt.Errorf("value %v is neither time.Time nor int64 type", value)
}

func (TimestampType) Parse(data []byte) interface{} {
	if len(data) < 8 {
		return nil
	}
	return time.Unix(0, *(*int64)(unsafe.Pointer(&data[0])))
}

func (TimestampType) String() string {
	return "timestamp"
}
