package statement

import (
	"errors"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/sql-parser/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type LoginStatement struct {
	Timestamp int64  `msgpack:"timestamp" json:"timestamp" valid:"required"`
	Token     string `msgpack:"token" json:"token" valid:"required"`
}

func NewLoginStatement(timestamp int64, token string) (*LoginStatement, error) {
	if timestamp <= 0 {
		return nil, errors.New("timestamp must be a valid Unix timestamp")
	}

	if token == "" {
		return nil, errors.New("token cannot be empty")
	}

	stmt := &LoginStatement{
		Timestamp: timestamp,
		Token:     token,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (l *LoginStatement) Protocol() protocol.MessageType {
	return protocol.Login
}

func (l *LoginStatement) Serialize() ([]byte, error) {
	return msgpack.Marshal(l)
}

func (l *LoginStatement) Deserialize(data []byte) error {
	return msgpack.Unmarshal(data, l)
}
