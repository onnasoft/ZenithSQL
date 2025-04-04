package statement

import (
	"errors"
	"fmt"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/core/utils"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

type JoinClusterStatement struct {
	Timestamp uint64   `msgpack:"timestamp" json:"timestamp"`
	IsReplica bool     `msgpack:"is_replica" json:"is_replica"`
	Hash      string   `msgpack:"hash" json:"hash"`
	NodeName  string   `msgpack:"node_name" json:"node_name"`
	NodeID    string   `msgpack:"node_id" json:"node_id"`
	Address   string   `msgpack:"address" json:"address"`
	Tags      []string `msgpack:"tags" json:"tags"`
}

func NewJoinClusterStatement(token, nodeID, nodeName string, isReplica bool, tags []string) (*JoinClusterStatement, error) {
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
	hash := utils.GenerateHash(token, timestamp, nodeID, isReplica, tags)

	stmt := &JoinClusterStatement{
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

func (j JoinClusterStatement) ValidateHash(token string) bool {
	return j.Hash == utils.GenerateHash(token, j.Timestamp, j.NodeID, j.IsReplica, j.Tags)
}

func (j JoinClusterStatement) Protocol() protocol.MessageType {
	return protocol.JoinCluster
}

func (j JoinClusterStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(j)
}

func (j *JoinClusterStatement) FromBytes(data []byte) error {
	return msgpack.Unmarshal(data, j)
}

func (j JoinClusterStatement) String() string {
	return fmt.Sprintf(
		"JoinClusterStatement{NodeID: %s, NodeName: %s, Address: %s, Tags: %v}",
		j.NodeID, j.NodeName, j.Address, j.Tags,
	)
}
