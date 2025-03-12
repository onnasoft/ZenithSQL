package statement

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/sql-parser/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type LoginStatement struct {
	Timestamp uint64 `msgpack:"timestamp" json:"timestamp"`
	NodeName  string `msgpack:"node_name" json:"node_name" valid:"required,alphanumunderscore"`
	IsReplica bool   `msgpack:"is_replica" json:"is_replica"`
	Hash      string `msgpack:"hash" json:"hash"`
}

func NewLoginStatement(token, nodeName string, isReplica bool) (*LoginStatement, error) {
	if token == "" {
		return nil, errors.New("token cannot be empty")
	}
	if nodeName == "" || !govalidator.Matches(nodeName, "^[a-zA-Z0-9_]+$") {
		return nil, errors.New("node name must be alphanumeric with underscores only")
	}

	timestamp := uint64(time.Now().UnixNano())
	hash := generateHash(token, timestamp, nodeName, isReplica)

	stmt := &LoginStatement{
		Timestamp: timestamp,
		NodeName:  nodeName,
		IsReplica: isReplica,
		Hash:      hash,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (l *LoginStatement) ValidateHash(token string) bool {
	expectedHash := generateHash(token, l.Timestamp, l.NodeName, l.IsReplica)
	return hmac.Equal([]byte(l.Hash), []byte(expectedHash))
}

func generateHash(token string, timestamp uint64, nodeName string, isReplica bool) string {
	h := hmac.New(sha256.New, []byte(token))
	h.Write([]byte(fmt.Sprintf("%d|%s|%t", timestamp, nodeName, isReplica)))
	return hex.EncodeToString(h.Sum(nil))
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
