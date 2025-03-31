package validate_test

import (
	"strings"
	"testing"

	"github.com/onnasoft/ZenithSQL/validate"
)

func TestHasLowerCase(t *testing.T) {
	v := validate.HasLowerCase{}

	tests := []struct {
		name    string
		value   interface{}
		wantErr bool
	}{
		{"all lowercase", "abc", false},
		{"all uppercase", "ABC", true},
		{"mixed case", "aBC", false},
		{"starts lowercase", "aBCdef", false},
		{"ends lowercase", "ABCdef", false},
		{"middle lowercase", "ABcDE", false},
		{"numbers only", "123", true},
		{"numbers with lowercase", "a1b2", false},
		{"symbols only", "@#$", true},
		{"symbols with lowercase", "a@b#", false},
		{"empty string", "", true},
		{"nil value", nil, true},
		{"non-string type", 123, true},
		{"unicode lowercase", "αβγ", false},
		{"unicode uppercase", "ΑΒΓ", true},
		{"whitespace only", " \t\n", true},
		{"whitespace with lowercase", " \ta\n", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(tt.value, "test_col")
			if (err != nil) != tt.wantErr {
				t.Errorf("%s: Validate(%v) error = %v, wantErr %v",
					tt.name, tt.value, err, tt.wantErr)
			}

			// Verify error message contains column name when error occurs
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
