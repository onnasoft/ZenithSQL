package validate

import (
	"fmt"
	"hash/crc32"
)

type IsCRC32b struct {
	Expected uint32
}

func (v IsCRC32b) Validate(value interface{}, colName string) error {
	table := crc32.MakeTable(crc32.Castagnoli)
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsCRC32b validation", colName)
	}
	actual := crc32.Checksum([]byte(str), table)
	if actual != v.Expected {
		return fmt.Errorf("column '%s' failed CRC32b check", colName)
	}
	return nil
}
