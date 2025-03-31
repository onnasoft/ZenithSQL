package validate_test

import (
	"testing"

	"github.com/onnasoft/ZenithSQL/validate"
)

func TestIsAlpha(t *testing.T) {
	v := validate.IsAlpha{}

	tests := []struct {
		name    string
		value   interface{}
		wantErr bool
	}{
		{"ASCII lowercase only", "abc", false},
		{"ASCII uppercase only", "XYZ", false},
		{"ASCII mixed case", "aBcDeF", false},
		{"contains numbers", "abc123", true},
		{"contains spaces", "abc def", true},
		{"contains symbols", "abc-def", true},
		{"empty string", "", true},
		{"nil value", nil, true},
		{"non-string type", 12345, true},
		{"non-alphabetic Unicode", "日本語", true},
		{"alphabetic Unicode", "αβγ", true}, // Change to false if you want to allow Unicode letters
		{"accented letters", "éñü", true},   // Change to false if you want to allow accented letters
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(tt.value, "test_column")
			if (err != nil) != tt.wantErr {
				t.Errorf("%s: error = %v, wantErr %v", tt.name, err, tt.wantErr)
			}

			// Additional check for non-empty error messages when error is expected
			if tt.wantErr && err != nil && err.Error() == "" {
				t.Errorf("%s: expected non-empty error message", tt.name)
			}
		})
	}
}
