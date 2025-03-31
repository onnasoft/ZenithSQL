package validate_test

import (
	"strings"
	"testing"

	"github.com/onnasoft/ZenithSQL/validate"
)

func TestHasUpperCase(t *testing.T) {
	v := validate.HasUpperCase{}

	tests := []struct {
		name    string
		value   interface{}
		wantErr bool
	}{
		{"all lowercase", "abc", true},
		{"all uppercase", "ABC", false},
		{"mixed case first upper", "Abc", false},
		{"mixed case middle upper", "aBc", false},
		{"mixed case last upper", "abC", false},
		{"numbers only", "123", true},
		{"numbers with upper", "A1B2", false},
		{"symbols only", "@#$", true},
		{"symbols with upper", "A@B#", false},
		{"empty string", "", true},
		{"nil value", nil, true},
		{"non-string type", 123, true},
		{"unicode lowercase", "αβγ", true},
		{"unicode uppercase", "ΑΒΓ", false},
		{"whitespace only", " \t\n", true},
		{"whitespace with upper", " \tA\n", false},
		{"single uppercase", "A", false},
		{"single lowercase", "a", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(tt.value, "test_col")
			if (err != nil) != tt.wantErr {
				t.Errorf("%s: Validate(%v) error = %v, wantErr %v",
					tt.name, tt.value, err, tt.wantErr)
			}

			if err != nil {
				if err.Error() == "" {
					t.Error("Error message should not be empty")
				}
				if !strings.Contains(err.Error(), "test_col") {
					t.Error("Error message should contain column name")
				}
			}
		})
	}
}
