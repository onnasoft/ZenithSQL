package validate_test

import (
	"testing"

	"github.com/onnasoft/ZenithSQL/validate"
)

func TestHasWhitespace(t *testing.T) {
	v := validate.HasWhitespace{}

	tests := []struct {
		value   interface{}
		wantErr bool
	}{
		{"hello world", false},
		{"helloworld", true},
		{"hello\tworld", false},
		{true, true},
	}

	for _, tt := range tests {
		err := v.Validate(tt.value, "col")
		if (err != nil) != tt.wantErr {
			t.Errorf("HasWhitespace.Validate(%v) = %v, wantErr %v", tt.value, err, tt.wantErr)
		}
	}
}
