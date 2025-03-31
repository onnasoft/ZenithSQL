package validate_test

import (
	"testing"

	"github.com/onnasoft/ZenithSQL/validate"
)

func TestIsASCII(t *testing.T) {
	v := validate.IsASCII{}

	tests := []struct {
		value   interface{}
		wantErr bool
	}{
		{"abc123", false},
		{"©abc", true},
		{123, true},
	}

	for _, tt := range tests {
		err := v.Validate(tt.value, "col")
		if (err != nil) != tt.wantErr {
			t.Errorf("IsASCII.Validate(%v) = %v, wantErr %v", tt.value, err, tt.wantErr)
		}
	}
}
