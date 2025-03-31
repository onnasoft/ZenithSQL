package validate_test

import (
	"testing"

	"github.com/onnasoft/ZenithSQL/validate"
)

func TestInRangeFloat64(t *testing.T) {
	v := validate.InRangeFloat64{Min: 0.0, Max: 1.0}

	tests := []struct {
		value   interface{}
		wantErr bool
	}{
		{0.5, false},
		{1.1, true},
		{float32(0.5), true},
		{"0.5", true},
	}

	for _, tt := range tests {
		err := v.Validate(tt.value, "col")
		if (err != nil) != tt.wantErr {
			t.Errorf("InRangeFloat64.Validate(%v) = %v, wantErr %v", tt.value, err, tt.wantErr)
		}
	}
}
