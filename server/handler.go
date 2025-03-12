package server

import (
	"encoding/hex"
	"net"
	"time"

	"github.com/onnasoft/sql-parser/protocol"
	"github.com/onnasoft/sql-parser/transport"
)

func (s *MessageServer) handleConnection(conn net.Conn) {
	s.logger.Info("New connection:", conn.RemoteAddr())

	if !s.authenticateConnection(conn) {
		s.logger.Warn("Authentication failed for:", conn.RemoteAddr())
		conn.Close()
		return
	}

	s.mu.Lock()
	s.connections[conn] = struct{}{}
	s.mu.Unlock()

	go s.handleIncomingMessages(conn)

	_, err := s.SendMessage(conn, transport.NewMessage(protocol.Welcome, []byte("Welcome to the server!")))
	if err != nil {
		s.logger.Error("Error sending welcome message:", err)
		s.closeConnection(conn)
	}
}

func (s *MessageServer) authenticateConnection(conn net.Conn) bool {
	conn.SetDeadline(time.Now().Add(10 * time.Second)) // 10s para autenticarse

	headerBytes := make([]byte, transport.MessageHeaderSize)
	if _, err := conn.Read(headerBytes); err != nil {
		s.logger.Error("Error reading login header:", err)
		return false
	}

	var header transport.MessageHeader
	if err := header.Deserialize(headerBytes); err != nil {
		s.logger.Error("Error deserializing login header:", err)
		return false
	}

	if header.MessageType != protocol.Login {
		s.logger.Warn("Invalid first message type from:", conn.RemoteAddr())
		return false
	}

	body := make([]byte, header.BodySize)
	if _, err := conn.Read(body); err != nil {
		s.logger.Error("Error reading login body:", err)
		return false
	}

	token := string(body)

	if s.loginValidator != nil && !s.loginValidator(token) {
		s.logger.Warn("Invalid token from:", conn.RemoteAddr())
		return false
	}

	s.logger.Info("Client authenticated:", conn.RemoteAddr())
	return true
}

func (s *MessageServer) handleIncomingMessages(conn net.Conn) {
	defer s.closeConnection(conn)

	for {
		conn.SetDeadline(time.Now().Add(30 * time.Second))

		headerBytes := make([]byte, transport.MessageHeaderSize)
		if _, err := conn.Read(headerBytes); err != nil {
			s.logger.Error("Error reading header:", err)
			return
		}

		var header transport.MessageHeader
		if err := header.Deserialize(headerBytes); err != nil {
			s.logger.Error("Error deserializing header:", err)
			return
		}

		body := make([]byte, header.BodySize)
		if _, err := conn.Read(body); err != nil {
			s.logger.Error("Error reading body:", err)
			return
		}

		message := &transport.Message{
			Header: header,
			Body:   body,
		}

		messageID := hex.EncodeToString(message.Header.MessageID[:])

		s.mu.Lock()
		if responseChan, exists := s.responseMap[messageID]; exists {
			responseChan <- message
			delete(s.responseMap, messageID)
		} else if s.messageHandler != nil {
			go s.messageHandler(conn, message)
		}
		s.mu.Unlock()
	}
}
