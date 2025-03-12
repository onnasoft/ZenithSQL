package server

import (
	"encoding/hex"
	"fmt"
	"net"
	"strings"

	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/onnasoft/ZenithSQL/statement"
	"github.com/onnasoft/ZenithSQL/transport"
)

// ✅ Captura errores en cada función
func recoverFromPanic(funcName string, s *MessageServer) {
	if r := recover(); r != nil {
		s.logger.Fatal(fmt.Sprintf("[PANIC] Recovered in %s: %v", funcName, r))
	}
}

// 📌 Manejo de conexión
func (s *MessageServer) handleConnection(conn net.Conn) {
	defer recoverFromPanic("handleConnection", s)

	handler := NewConnectionHandler(conn, s)
	nodeID, _, authenticated := s.authenticateConnection(handler)
	if !authenticated {
		s.logger.Warn("Authentication failed for:", conn.RemoteAddr())
		conn.Close()
		return
	}

	s.registerNode(nodeID, handler)

	s.logger.Info("Connection established for node: ", nodeID, " from:", conn.RemoteAddr())

	for {
		message := handler.ReadMessage()
		if message == nil {
			break
		}

		s.routeMessage(handler, message)
	}
}

func (s *MessageServer) authenticateConnection(conn *ConnectionHandler) (string, []string, bool) {
	defer recoverFromPanic("authenticateConnection", s)

	message := conn.ReadMessage()
	if message == nil {
		return "", nil, false
	}

	stmt := message.Stmt.(*statement.LoginStatement)

	if s.loginValidator != nil && !s.loginValidator(stmt) {
		s.logger.Warn("Invalid token for node:", stmt.NodeID, "from:", conn.RemoteAddr())
		return "", nil, false
	}

	response, _ := transport.NewMessage(protocol.Login, statement.NewEmptyStatement(protocol.Login))
	response.Header.MessageID = message.Header.MessageID
	response.Header.Timestamp = message.Header.Timestamp
	err := s.SendSilentMessage(conn, response)
	if err != nil {
		s.logger.Warn("Failed to send login response to:", conn.RemoteAddr())
		return "", nil, false
	}

	s.logger.Info("Authenticated node:", stmt.NodeID, "Tags:", stmt.Tags)
	return stmt.NodeID, stmt.Tags, true
}

func (s *MessageServer) registerNode(nodeID string, conn *ConnectionHandler) {
	defer recoverFromPanic("registerNode", s)

	var role NodeRole
	if strings.Contains(nodeID, "master") {
		role = Master
	} else {
		role = Slave
	}

	conn.NodeID = nodeID
	node := s.nodeManager.GetNode(nodeID)
	if node == nil {
		node = s.nodeManager.AddNode(nodeID, role)
	}
	node.AddConnection(conn)
}

func (s *MessageServer) handlePingMessage(conn *ConnectionHandler, message *transport.Message) bool {
	defer recoverFromPanic("handlePingMessage", s)

	if message.Header.MessageType != protocol.Ping {
		return false
	}

	pongMessage, _ := transport.NewMessage(protocol.Pong, statement.NewEmptyStatement(protocol.Pong))
	pongMessage.Header.MessageID = message.Header.MessageID
	_ = s.SendSilentMessage(conn, pongMessage)
	return true
}

func (s *MessageServer) routeMessage(conn *ConnectionHandler, message *transport.Message) {
	defer recoverFromPanic("routeMessage", s)

	messageID := hex.EncodeToString(message.Header.MessageID[:])

	s.mu.Lock()
	responseChan, exists := s.responseMap[messageID]
	s.mu.Unlock()

	if exists {
		responseChan <- message
		s.mu.Lock()
		delete(s.responseMap, messageID)
		s.mu.Unlock()
	} else if s.messageHandler != nil {
		go s.messageHandler(conn, message)
	}

	node := s.nodeManager.GetNodeByConnection(conn)
	if node != nil {
		for _, replica := range node.Replicas {
			_ = s.SendSilentMessage(replica, message)
		}
	}
}
