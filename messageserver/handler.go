package messageserver

import (
	"net"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/network"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/onnasoft/ZenithSQL/response"
	"github.com/onnasoft/ZenithSQL/statement"
	"github.com/onnasoft/ZenithSQL/transport"
	"github.com/onnasoft/ZenithSQL/utils"
)

func (s *MessageServer) handleConnection(conn net.Conn) {
	defer utils.RecoverFromPanic("handleConnection", s.logger)

	stmt, authenticated := s.authenticateConnection(conn)
	if !authenticated {
		s.logger.Warn("Authentication failed for: ", conn.RemoteAddr())
		conn.Close()
		return
	}

	loginStmt := stmt.(*statement.JoinClusterStatement)

	handler := network.NewZenithConnection(conn, s.logger, s.timeout)

	if s.onConnection != nil {
		s.onConnection(handler, loginStmt)
	}

	s.registerNode(loginStmt, handler)
	s.processMessage(handler)
}

func (s *MessageServer) processMessage(conn *network.ZenithConnection) {
	defer utils.RecoverFromPanic("processMessage", s.logger)
	defer conn.Close()

	for {
		message := new(transport.Message)
		if err := message.ReadFrom(conn); err != nil {
			break
		}

		go s.handler(conn, message)
	}
}

func (s *MessageServer) handler(conn *network.ZenithConnection, message *transport.Message) {
	if s.handlePing(conn, message) {
		return
	}

	s.handlerMessage(conn, message)
}

func (s *MessageServer) handlerMessage(conn *network.ZenithConnection, message *transport.Message) {
	defer utils.RecoverFromPanic("handler", s.logger)

	if s.messageHandler != nil {
		s.messageHandler(conn, message)
		return
	}
}

func (s *MessageServer) handlePing(conn net.Conn, message *transport.Message) bool {
	defer utils.RecoverFromPanic("handlePing", s.logger)

	if message.Header.MessageType != protocol.Ping {
		return false
	}

	response, _ := transport.NewResponseMessage(message, response.NewPongResponse())
	conn.Write(response.ToBytes())

	return true
}

func (s *MessageServer) authenticateConnection(conn net.Conn) (statement.Statement, bool) {
	defer utils.RecoverFromPanic("authenticateConnection", s.logger)
	message := new(transport.Message)

	if err := message.ReadFrom(conn); err != nil {
		s.logger.Warn("Failed to read message, error: ", err)
		return nil, false
	}

	stmt := new(statement.JoinClusterStatement)
	if err := stmt.FromBytes(message.Body); err != nil {
		s.logger.Warn("Failed to parse login statement, error: ", err)
		return nil, false
	}

	if _, err := govalidator.ValidateStruct(stmt); err != nil {
		s.logger.Warn("Invalid login statement: ", err)
		return nil, false
	}

	if s.joinValidator != nil && !s.joinValidator(stmt) {
		s.logger.Warn("Invalid token for node:", stmt.NodeID, "from:", conn.RemoteAddr())
		return nil, false
	}

	response, _ := transport.NewResponseMessage(message, response.NewLoginResponse(true, "Login successful"))
	_, err := conn.Write(response.ToBytes())
	if err != nil {
		s.logger.Warn("Failed to send login response to:", conn.RemoteAddr())
		return nil, false
	}

	return stmt, true
}
