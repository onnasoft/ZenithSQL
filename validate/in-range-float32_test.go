package validate_test

import (
	"testing"

	"github.com/onnasoft/ZenithSQL/validate"
)

func TestInRangeFloat32(t *testing.T) {
	v := validate.InRangeFloat32{Min: 1.0, Max: 10.0}

	tests := []struct {
		value   interface{}
		wantErr bool
	}{
		{float32(5.5), false},
		{float32(0.9), true},
		{10.1, true},
		{"x", true},
	}

	for _, tt := range tests {
		err := v.Validate(tt.value, "col")
		if (err != nil) != tt.wantErr {
			t.Errorf("InRangeFloat32.Validate(%v) = %v, wantErr %v", tt.value, err, tt.wantErr)
		}
	}
}
