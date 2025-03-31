package validate_test

import (
	"testing"

	"github.com/onnasoft/ZenithSQL/validate"
)

func TestInRangeInt(t *testing.T) {
	v := validate.InRangeInt{Min: 1, Max: 10}

	tests := []struct {
		value   interface{}
		wantErr bool
	}{
		{5, false},
		{1, false},
		{10, false},
		{0, true},
		{11, true},
		{"x", true},
	}

	for _, tt := range tests {
		err := v.Validate(tt.value, "col")
		if (err != nil) != tt.wantErr {
			t.Errorf("InRangeInt.Validate(%v) = %v, wantErr %v", tt.value, err, tt.wantErr)
		}
	}
}
