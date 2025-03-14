package transport

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/onnasoft/ZenithSQL/statement"
)

const (
	StartMarker       uint32 = 0xDEADBEEF
	EndMarker         uint32 = 0xBEEFDEAD
	MessageHeaderSize        = 40

	// Offsets dentro del MessageHeader
	StartMarkerOffset = 0
	MessageIDOffset   = 4
	MessageTypeOffset = 20
	TimestampOffset   = 24
	BodySizeOffset    = 32
	EndMarkerOffset   = 36
)

type MessageHeader struct {
	StartMarker uint32
	MessageID   [16]byte
	messageID   string
	MessageType protocol.MessageType
	Timestamp   uint64
	BodySize    uint32
	EndMarker   uint32
}

func (h *MessageHeader) MessageIDString() string {
	if h.messageID == "" {
		h.messageID = hex.EncodeToString(h.MessageID[:])
	}

	return h.messageID
}

func (h *MessageHeader) ReadFrom(conn net.Conn) error {
	headerBytes := make([]byte, MessageHeaderSize)
	if _, err := conn.Read(headerBytes); err != nil {
		return err
	}

	return h.FromBytes(headerBytes)
}

func (h *MessageHeader) ToBytes() []byte {
	bytes := make([]byte, MessageHeaderSize)

	binary.BigEndian.PutUint32(bytes[StartMarkerOffset:MessageIDOffset], h.StartMarker)
	copy(bytes[MessageIDOffset:MessageTypeOffset], h.MessageID[:])
	binary.BigEndian.PutUint32(bytes[MessageTypeOffset:TimestampOffset], uint32(h.MessageType))
	binary.BigEndian.PutUint64(bytes[TimestampOffset:BodySizeOffset], h.Timestamp)
	binary.BigEndian.PutUint32(bytes[BodySizeOffset:EndMarkerOffset], h.BodySize)
	binary.BigEndian.PutUint32(bytes[EndMarkerOffset:MessageHeaderSize], h.EndMarker)

	return bytes
}

func (h *MessageHeader) FromBytes(bytes []byte) error {
	if len(bytes) != MessageHeaderSize {
		return fmt.Errorf("header size must be %v bytes, got %v", MessageHeaderSize, len(bytes))
	}

	h.StartMarker = binary.BigEndian.Uint32(bytes[StartMarkerOffset:MessageIDOffset])
	if h.StartMarker != StartMarker {
		return fmt.Errorf("invalid start marker: expected 0xDEADBEEF, got 0x%X", h.StartMarker)
	}

	copy(h.MessageID[:], bytes[MessageIDOffset:MessageTypeOffset])
	h.MessageType = protocol.MessageType(binary.BigEndian.Uint32(bytes[MessageTypeOffset:TimestampOffset]))
	h.Timestamp = binary.BigEndian.Uint64(bytes[TimestampOffset:BodySizeOffset])
	h.BodySize = binary.BigEndian.Uint32(bytes[BodySizeOffset:EndMarkerOffset])
	h.EndMarker = binary.BigEndian.Uint32(bytes[EndMarkerOffset:MessageHeaderSize])

	if h.EndMarker != EndMarker {
		return fmt.Errorf("invalid end marker: expected 0xBEEFDEAD, got 0x%X", h.EndMarker)
	}

	return nil
}

type Message struct {
	Header *MessageHeader
	Body   []byte
	Stmt   statement.Statement
}

func NewMessage(messageType protocol.MessageType, stmt statement.Statement) (*Message, error) {
	body, err := stmt.ToBytes()
	if err != nil {
		return nil, err
	}

	return &Message{
		Header: &MessageHeader{
			StartMarker: StartMarker,
			MessageID:   uuid.New(),
			MessageType: messageType,
			Timestamp:   uint64(time.Now().UnixNano()),
			BodySize:    uint32(len(body)),
			EndMarker:   EndMarker,
		},
		Body: body,
	}, nil
}

func NewResponseMessage(request *Message, stmt statement.Statement) (*Message, error) {
	body, err := stmt.ToBytes()
	if err != nil {
		return nil, err
	}

	return &Message{
		Header: &MessageHeader{
			StartMarker: StartMarker,
			MessageID:   request.Header.MessageID,
			MessageType: stmt.Protocol(),
			Timestamp:   uint64(time.Now().UnixNano()),
			BodySize:    uint32(len(body)),
			EndMarker:   EndMarker,
		},
		Body: body,
		Stmt: stmt,
	}, nil
}

func (m *Message) ReadFrom(conn net.Conn) error {
	if m.Header == nil {
		m.Header = &MessageHeader{}
	}

	if err := m.Header.ReadFrom(conn); err != nil {
		return err
	}

	m.Body = make([]byte, m.Header.BodySize)
	if _, err := conn.Read(m.Body); err != nil {
		return err
	}

	stmt, err := statement.DeserializeStatement(m.Header.MessageType, m.Body)
	if err != nil {
		return err
	}

	m.Stmt = stmt
	return nil
}

func ParseStatement(header *MessageHeader, body []byte) (*Message, error) {
	stmt, err := statement.DeserializeStatement(header.MessageType, body)
	if err != nil {
		return nil, err
	}

	return &Message{
		Header: header,
		Body:   body,
		Stmt:   stmt,
	}, nil
}

func (m *Message) ToBytes() []byte {
	headerBytes := m.Header.ToBytes()
	return append(headerBytes, m.Body...)
}

func (m *Message) FromBytes(bytes []byte) error {
	if len(bytes) < MessageHeaderSize {
		return fmt.Errorf("message size too small")
	}

	if err := m.Header.FromBytes(bytes[:MessageHeaderSize]); err != nil {
		return err
	}

	m.Body = bytes[MessageHeaderSize:]
	if uint32(len(m.Body)) != m.Header.BodySize {
		return fmt.Errorf("body size mismatch: expected %d, got %d", m.Header.BodySize, len(m.Body))
	}

	return nil
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

	case protocol.Ping, protocol.Pong, protocol.Greeting, protocol.Welcome:
		return "UTILITY"

	default:
		return "UNKNOWN"
	}
}

func (m *Message) String() string {
	return m.Stmt.String()
}

func (m *Message) Clear() {
	m.Header = nil
	m.Body = nil
	m.Stmt = nil
}
