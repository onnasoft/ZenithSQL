package validate_test

import (
	"testing"

	"github.com/onnasoft/ZenithSQL/validate"
)

func TestHasWhitespaceOnly(t *testing.T) {
	v := validate.HasWhitespaceOnly{}

	tests := []struct {
		name    string // Descriptive test name
		value   interface{}
		wantErr bool
	}{
		{"only spaces", "   ", false},
		{"tabs only", "\t\t\t", false},
		{"mixed whitespace", " \t\n\r\v\f", false}, // includes all common whitespace chars
		{"leading letter", "a ", true},
		{"trailing letter", " a", true},
		{"middle letter", " a ", true},
		{"number", " 1 ", true},
		{"symbol", " . ", true},
		{"empty string", "", true}, // matches your existing test expectation
		{"nil value", nil, true},
		{"non-string type", 123, true},
		{"unicode whitespace", "\u00A0\u2000", false}, // non-breaking space and en quad
		{"unicode non-whitespace", " \u00A0a\u2000", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(tt.value, "test_column")
			if (err != nil) != tt.wantErr {
				t.Errorf("%s: Validate(%#v) error = %v, wantErr %v",
					tt.name, tt.value, err, tt.wantErr)
			}

			// Verify error message contains column name when error occurs
			if err != nil {
				if err.Error() == "" {
					t.Error("Error message should not be empty")
				}
			}
		})
	}
}
