package validate_test

import (
	"strings"
	"testing"

	"github.com/onnasoft/ZenithSQL/validate"
)

func TestIsEmail(t *testing.T) {
	v := validate.IsEmail{}

	tests := []struct {
		name    string
		value   interface{}
		wantErr bool
	}{
		// Valid emails
		{"standard email", "test@example.com", false},
		{"uppercase letters", "TEST@example.com", false},
		{"subdomain", "test@sub.example.com", false},
		{"numbers", "test123@example.com", false},
		{"special chars", "t.e.s.t@example.com", false},
		{"plus addressing", "test+filter@example.com", false},
		{"international", "用户@例子.测试", false},

		// Invalid emails
		{"missing @", "example.com", true},
		{"missing domain", "test@", true},
		{"missing local", "@example.com", true},
		{"space in address", "test @example.com", true},
		{"invalid chars", "test@exa mple.com", true},
		{"too long", strings.Repeat("a", 250) + "@example.com", true},
		{"empty string", "", true},
		{"nil value", nil, true},
		{"non-string type", 123, true},

		// Edge cases
		{"minimum valid", "a@b", false},
		{"quoted local", `"test"@example.com`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(tt.value, "email_col")
			if (err != nil) != tt.wantErr {
				t.Errorf("%s: Validate(%v) error = %v, wantErr %v",
					tt.name, tt.value, err, tt.wantErr)
			}

			// Additional error checks
			if err != nil {
				if !strings.Contains(err.Error(), "email_col") {
					t.Error("Error message should contain column name")
				}
			}
		})
	}
}
