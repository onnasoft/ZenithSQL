package validate

import (
	"fmt"
	"hash/crc32"
)

type IsCRC32 struct {
	Expected uint32
}

func (v IsCRC32) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsCRC32 validation", colName)
	}
	actual := crc32.ChecksumIEEE([]byte(str))
	if actual != v.Expected {
		return fmt.Errorf("column '%s' failed CRC32 check", colName)
	}
	return nil
}
