package utils

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

func GenerateHash(token string, timestamp uint64, nodeID string, isReplica bool, tags []string) string {
	h := hmac.New(sha256.New, []byte(token))
	h.Write([]byte(fmt.Sprintf("%d|%s|%t|%v", timestamp, nodeID, isReplica, strings.Join(tags, ","))))
	return hex.EncodeToString(h.Sum(nil))
}
