package transport

import (
	"encoding/binary"
	"errors"

	"github.com/google/uuid"
	"github.com/onnasoft/sql-parser/protocol"
)

type MessageHeader struct {
	MessageID   [16]byte
	MessageType protocol.MessageType
	BodySize    uint32
}

func (h *MessageHeader) ToBytes() []byte {
	bytes := make([]byte, 24)
	copy(bytes[:16], h.MessageID[:])
	binary.BigEndian.PutUint32(bytes[16:20], uint32(h.MessageType))
	binary.BigEndian.PutUint32(bytes[20:24], h.BodySize)
	return bytes
}

func (h *MessageHeader) FromBytes(bytes []byte) error {
	if len(bytes) != 24 {
		return errors.New("header size must be 24 bytes")
	}
	copy(h.MessageID[:], bytes[:16])
	h.MessageType = protocol.MessageType(binary.BigEndian.Uint32(bytes[16:20]))
	h.BodySize = binary.BigEndian.Uint32(bytes[20:24])
	return nil
}

type Message struct {
	Header MessageHeader
	Body   []byte
}

func NewMessage(messageType protocol.MessageType, body []byte) *Message {
	return &Message{
		Header: MessageHeader{
			MessageID:   uuid.New(),
			MessageType: messageType,
			BodySize:    uint32(len(body)),
		},
		Body: body,
	}
}

func (m *Message) ToBytes() []byte {
	headerBytes := m.Header.ToBytes()
	return append(headerBytes, m.Body...)
}

func (m *Message) OperationType() string {
	switch m.Header.MessageType {
	case protocol.CreateDatabase, protocol.DropDatabase, protocol.ShowDatabases,
		protocol.CreateTable, protocol.DropTable, protocol.AlterTable, protocol.RenameTable, protocol.TruncateTable, protocol.ShowTables, protocol.DescribeTable,
		protocol.CreateIndex, protocol.DropIndex, protocol.ShowIndexes:
		return "DDL"

	case protocol.Insert, protocol.Select, protocol.Update, protocol.Delete, protocol.BulkInsert, protocol.Upsert:
		return "DML"

	case protocol.BeginTransaction, protocol.Commit, protocol.Rollback, protocol.Savepoint, protocol.ReleaseSavepoint:
		return "TCL"

	// Data Control Language (DCL) - Control de permisos (posible expansión)
	// case protocol.Grant, protocol.Revoke:
	// 	return "DCL"

	// Utility commands
	case protocol.Ping, protocol.Pong, protocol.Greeting, protocol.Welcome:
		return "UTILITY"

	default:
		return "UNKNOWN"
	}
}
