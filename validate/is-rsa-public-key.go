package validate

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

type IsRsaPublicKey struct {
	KeyLen int
}

func (v IsRsaPublicKey) Validate(value interface{}, colName string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for RSA public key validation", colName)
	}
	block, _ := pem.Decode([]byte(str))
	if block == nil {
		return fmt.Errorf("column '%s' must be a valid PEM block", colName)
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("column '%s' contains an invalid RSA public key", colName)
	}
	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok || rsaPub.N.BitLen() != v.KeyLen {
		return fmt.Errorf("column '%s' RSA key length must be %d bits", colName, v.KeyLen)
	}
	return nil
}
