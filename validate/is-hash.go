package validate

import (
	"fmt"
	"strings"
)

type IsHash struct {
	Algorithm string
}

func (v IsHash) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsHash validation", colName)
	}
	length := len(str)

	algo := strings.ToLower(v.Algorithm)
	valid := map[string]int{
		"md4":       32,
		"md5":       32,
		"sha1":      40,
		"sha256":    64,
		"sha384":    96,
		"sha512":    128,
		"ripemd128": 32,
		"ripemd160": 40,
		"tiger128":  32,
		"tiger160":  40,
		"tiger192":  48,
	}

	expectedLen, ok := valid[algo]
	if !ok || length != expectedLen {
		return fmt.Errorf("column '%s' must be a valid %s hash", colName, v.Algorithm)
	}
	return nil
}
