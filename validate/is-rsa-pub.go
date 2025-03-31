package validate

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

type IsRsaPub struct{}

func (v IsRsaPub) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for RSA public key validation", colName)
	}
	block, _ := pem.Decode([]byte(str))
	if block == nil || block.Type != "PUBLIC KEY" {
		return fmt.Errorf("column '%s' must be a valid PEM encoded RSA public key", colName)
	}
	_, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("column '%s' contains an invalid RSA public key", colName)
	}
	return nil
}
