package server

import (
	"encoding/hex"
	"net"
	"time"

	"github.com/onnasoft/sql-parser/protocol"
	"github.com/onnasoft/sql-parser/statement"
	"github.com/onnasoft/sql-parser/transport"
)

func (s *MessageServer) handleConnection(conn net.Conn) {
	s.logger.Info("New connection:", conn.RemoteAddr())

	// Autenticación del nodo
	nodeID, tags, authenticated := s.authenticateConnection(conn)
	if !authenticated {
		s.logger.Warn("Authentication failed for:", conn.RemoteAddr())
		s.closeConnection(conn, "authentication error")
		return
	}

	// Registrar nodo si no existe
	s.mu.Lock()
	node, exists := s.nodes[nodeID]
	if !exists {
		node = NewNode(nodeID, tags)
		s.nodes[nodeID] = node
	}
	node.AddConnection(conn)
	s.mu.Unlock()

	s.logger.Info("Node registered:", nodeID, "Tags:", tags, "Connection from:", conn.RemoteAddr())

	go s.handleIncomingMessages(conn)
}

func (s *MessageServer) authenticateConnection(conn net.Conn) (string, []string, bool) {
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	header, body, err := s.readMessage(conn)
	if err != nil {
		s.logger.Error("Error reading login message:", err)
		return "", nil, false
	}

	if header.MessageType != protocol.Login {
		s.logger.Warn("Invalid first message type from:", conn.RemoteAddr())
		return "", nil, false
	}

	stmt := &statement.LoginStatement{}
	if err := stmt.FromBytes(body); err != nil {
		s.logger.Error("Error parsing message:", err)
		return "", nil, false
	}

	if s.loginValidator != nil && !s.loginValidator(stmt) {
		s.logger.Warn("Invalid token from:", conn.RemoteAddr())
		return "", nil, false
	}

	response, _ := transport.NewMessage(protocol.Login, statement.NewEmptyStatement(protocol.Login))
	response.Header.MessageID = header.MessageID

	if err := s.SendSilentMessage(conn, response); err != nil {
		s.logger.Error("Error sending login response:", err)
		return "", nil, false
	}

	s.logger.Info("Client authenticated:", conn.RemoteAddr(), "Node ID:", stmt.NodeID, "Tags:", stmt.Tags)
	return stmt.NodeID, stmt.Tags, true
}

func (s *MessageServer) handleIncomingMessages(conn net.Conn) {
	defer s.closeConnection(conn, "connection closed")

	for {
		conn.SetDeadline(time.Now().Add(30 * time.Second))

		header, body, err := s.readMessage(conn)
		if err != nil {
			s.logger.Error("Error reading message:", err)
			return
		}

		message, err := transport.ParseStatement(header, body)
		if err != nil {
			s.logger.Error("Error parsing message:", err)
			return
		}

		messageID := hex.EncodeToString(message.Header.MessageID[:])

		if s.handlePingMessage(conn, message) {
			continue
		}

		s.processMessage(conn, messageID, message)
	}
}

func (s *MessageServer) readMessage(conn net.Conn) (*transport.MessageHeader, []byte, error) {
	headerBytes := make([]byte, transport.MessageHeaderSize)
	if _, err := conn.Read(headerBytes); err != nil {
		s.logger.Error("Error reading header:", err)
		return nil, nil, err
	}

	header := &transport.MessageHeader{}
	if err := header.Deserialize(headerBytes); err != nil {
		s.logger.Error("Error deserializing header:", err)
		return nil, nil, err
	}

	body := make([]byte, header.BodySize)
	if _, err := conn.Read(body); err != nil {
		s.logger.Error("Error reading body:", err)
		return nil, nil, err
	}

	return header, body, nil
}

func (s *MessageServer) handlePingMessage(conn net.Conn, message *transport.Message) bool {
	if message.Header.MessageType == protocol.Ping {
		pongMessage, _ := transport.NewMessage(protocol.Pong, statement.NewEmptyStatement(protocol.Pong))
		pongMessage.Header.MessageID = message.Header.MessageID
		if err := s.SendSilentMessage(conn, pongMessage); err != nil {
			s.logger.Error("Error sending PONG:", err)
		}
		return true
	}
	return false
}

func (s *MessageServer) processMessage(conn net.Conn, messageID string, message *transport.Message) {
	s.mu.Lock()
	responseChan, exists := s.responseMap[messageID]
	s.mu.Unlock()

	if exists {
		responseChan <- message
		delete(s.responseMap, messageID)
	} else if s.messageHandler != nil {
		go s.messageHandler(conn, message)
	}
}
