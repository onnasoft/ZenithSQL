package transport

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/onnasoft/ZenithSQL/io/dto"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/onnasoft/ZenithSQL/io/response"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

const (
	StartMarker       uint32 = 0xDEADBEEF
	EndMarker         uint32 = 0xBEEFDEAD
	MessageHeaderSize        = 37
)

type MessageTypeFlag uint8

const (
	RequestMessage  MessageTypeFlag = 1
	ResponseMessage MessageTypeFlag = 2
)

type MessageHeader struct {
	StartMarker uint32
	MessageID   [16]byte
	MessageType protocol.MessageType
	MessageFlag MessageTypeFlag
	Timestamp   uint32
	BodySize    uint32
	EndMarker   uint32
}

func (h *MessageHeader) MessageIDString() string {
	return hex.EncodeToString(h.MessageID[:])
}

func (h *MessageHeader) ToBytes() []byte {
	bytes := make([]byte, MessageHeaderSize)

	binary.BigEndian.PutUint32(bytes[0:4], h.StartMarker)
	copy(bytes[4:20], h.MessageID[:])
	binary.BigEndian.PutUint32(bytes[20:24], uint32(h.MessageType))
	bytes[24] = byte(h.MessageFlag)
	binary.BigEndian.PutUint32(bytes[25:29], h.Timestamp)
	binary.BigEndian.PutUint32(bytes[29:33], h.BodySize)
	binary.BigEndian.PutUint32(bytes[33:37], h.EndMarker)

	return bytes
}

func (h *MessageHeader) FromBytes(bytes []byte) error {
	if len(bytes) != MessageHeaderSize {
		return fmt.Errorf("header size must be %v bytes, got %v", MessageHeaderSize, len(bytes))
	}

	h.StartMarker = binary.BigEndian.Uint32(bytes[0:4])
	if h.StartMarker != StartMarker {
		return fmt.Errorf("invalid start marker: expected 0xDEADBEEF, got 0x%X", h.StartMarker)
	}

	copy(h.MessageID[:], bytes[4:20])
	h.MessageType = protocol.MessageType(binary.BigEndian.Uint32(bytes[20:24]))
	h.MessageFlag = MessageTypeFlag(bytes[24])
	h.Timestamp = binary.BigEndian.Uint32(bytes[25:29])
	h.BodySize = binary.BigEndian.Uint32(bytes[29:33])
	h.EndMarker = binary.BigEndian.Uint32(bytes[33:37])

	if h.EndMarker != EndMarker {
		return fmt.Errorf("invalid end marker: expected 0xBEEFDEAD, got 0x%X", h.EndMarker)
	}

	return nil
}

func (h *MessageHeader) ReadFrom(conn net.Conn) error {
	headerBytes := make([]byte, MessageHeaderSize)
	if _, err := conn.Read(headerBytes); err != nil {
		return err
	}

	return h.FromBytes(headerBytes)
}

type Message struct {
	Header *MessageHeader
	Body   []byte
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
			MessageFlag: RequestMessage,
			Timestamp:   uint32(time.Now().UnixNano() / 1e6),
			BodySize:    uint32(len(body)),
			EndMarker:   EndMarker,
		},
		Body: body,
	}, nil
}

func (m *Message) ReadFrom(conn net.Conn) error {
	var err error
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

	return err
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
			MessageType: request.Header.MessageType,
			MessageFlag: ResponseMessage,
			Timestamp:   uint32(time.Now().UnixNano() / 1e6),
			BodySize:    uint32(len(body)),
			EndMarker:   EndMarker,
		},
		Body: body,
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

	m.Header = &MessageHeader{}
	if err := m.Header.FromBytes(bytes[:MessageHeaderSize]); err != nil {
		return err
	}

	m.Body = bytes[MessageHeaderSize:]
	if uint32(len(m.Body)) != m.Header.BodySize {
		return fmt.Errorf("body size mismatch: expected %d, got %d", m.Header.BodySize, len(m.Body))
	}

	return nil
}

func (m *Message) DeserializeBody() (dto.Dto, error) {
	if m.Header.MessageFlag != RequestMessage {
		return response.Deserialize(m.Header.MessageType, m.Body)
	}

	return statement.Deserialize(m.Header.MessageType, m.Body)
}
