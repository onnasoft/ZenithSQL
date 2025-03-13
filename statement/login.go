package statement

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type LoginStatement struct {
	Timestamp uint64   `msgpack:"timestamp" json:"timestamp"`
	IsReplica bool     `msgpack:"is_replica" json:"is_replica"`
	Hash      string   `msgpack:"hash" json:"hash"`
	NodeName  string   `msgpack:"node_name" json:"node_name"`
	NodeID    string   `msgpack:"node_id" json:"node_id"`
	Address   string   `msgpack:"address" json:"address"`
	Tags      []string `msgpack:"tags" json:"tags"`
}

func NewLoginStatement(token, nodeID, nodeName string, isReplica bool, tags []string) (*LoginStatement, error) {
	if token == "" {
		return nil, errors.New("token cannot be empty")
	}
	if nodeID == "" || !govalidator.Matches(nodeID, "^[a-zA-Z0-9_-]+$") {
		return nil, errors.New("node ID must be alphanumeric with underscores or dashes")
	}
	if nodeName == "" || !govalidator.Matches(nodeName, "^[a-zA-Z0-9_]+$") {
		return nil, errors.New("node name must be alphanumeric with underscores only")
	}
	if len(tags) == 0 {
		return nil, errors.New("tags cannot be empty")
	}
	for _, tag := range tags {
		if tag == "" || !govalidator.Matches(tag, "^[a-zA-Z0-9_-]+$") {
			return nil, fmt.Errorf("invalid tag: %s", tag)
		}
	}

	timestamp := uint64(time.Now().UnixNano())
	hash := generateHash(token, timestamp, nodeID, isReplica, tags)

	stmt := &LoginStatement{
		Timestamp: timestamp,
		IsReplica: isReplica,
		Hash:      hash,
		NodeName:  nodeName,
		NodeID:    nodeID,
		Tags:      tags,
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (l *LoginStatement) ValidateHash(token string) bool {
	expectedHash := generateHash(token, l.Timestamp, l.NodeID, l.IsReplica, l.Tags)
	return hmac.Equal([]byte(l.Hash), []byte(expectedHash))
}

func generateHash(token string, timestamp uint64, nodeID string, isReplica bool, tags []string) string {
	h := hmac.New(sha256.New, []byte(token))
	h.Write([]byte(fmt.Sprintf("%d|%s|%t|%v", timestamp, nodeID, isReplica, tags)))
	return hex.EncodeToString(h.Sum(nil))
}

func (l *LoginStatement) Protocol() protocol.MessageType {
	return protocol.Login
}

func (l *LoginStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(l)
}

func (l *LoginStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, l)
}

func (l *LoginStatement) String() string {
	return fmt.Sprintf("LoginStatement{Timestamp: %d, NodeID: %s, NodeName: %s, IsReplica: %t, Tags: %v}",
		l.Timestamp, l.NodeID, l.NodeName, l.IsReplica, l.Tags)
}
