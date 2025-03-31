package validate_test

import (
	"testing"

	"github.com/onnasoft/ZenithSQL/validate"
)

func TestIsUpperCase(t *testing.T) {
	v := validate.IsUpperCase{}

	tests := []struct {
		value   interface{}
		wantErr bool
	}{
		{"HELLO", false},
		{"Hello", true},
		{"", false},
		{123, true},
	}

	for _, tt := range tests {
		err := v.Validate(tt.value, "test_col")
		if (err != nil) != tt.wantErr {
			t.Errorf("IsUpperCase.Validate(%v) = %v, wantErr %v", tt.value, err, tt.wantErr)
		}
	}
}
