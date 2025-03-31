package validate

import (
	"fmt"
	"unicode"
)

type IsVariableWidth struct{}

func (v IsVariableWidth) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string", colName)
	}
	hasWide := false
	hasNarrow := false
	for _, r := range str {
		if r <= '\uFF65' && (r >= '\uFF61' && r <= '\uFF9F') { // Half-width Katakana range
			hasNarrow = true
		} else if unicode.Is(unicode.Han, r) || (r >= '\uFF01' && r <= '\uFF60') { // Full-width range
			hasWide = true
		}
	}
	if !(hasWide && hasNarrow) {
		return fmt.Errorf("column '%s' must contain both full-width and half-width characters", colName)
	}
	return nil
}
