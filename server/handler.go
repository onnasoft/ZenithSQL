package server

import (
	"encoding/hex"
	"net"
	"time"

	"github.com/onnasoft/sql-parser/protocol"
	"github.com/onnasoft/sql-parser/transport"
)

func (s *MessageServer) handleConnection(conn net.Conn) {
	s.mu.Lock()
	s.connections[conn] = struct{}{}
	s.mu.Unlock()

	s.logger.Info("New connection:", conn.RemoteAddr())

	go s.handleIncomingMessages(conn)

	_, err := s.SendMessage(conn, transport.NewMessage(protocol.Welcome, []byte("Welcome to the server!")))
	if err != nil {
		s.logger.Error("Error sending welcome message:", err)
		s.closeConnection(conn)
	}
}

func (s *MessageServer) handleIncomingMessages(conn net.Conn) {
	defer s.closeConnection(conn)

	for {
		conn.SetDeadline(time.Now().Add(30 * time.Second)) // Timeout automático si no hay actividad

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

func (s *MessageServer) SendMessage(conn net.Conn, message *transport.Message) (*transport.Message, error) {
	_, err := conn.Write(message.Serialize())
	if err != nil {
		return nil, err
	}

	messageID := hex.EncodeToString(message.Header.MessageID[:])

	s.mu.Lock()
	responseChan := make(chan *transport.Message, 1)
	s.responseMap[messageID] = responseChan
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.responseMap, messageID)
		s.mu.Unlock()
	}()

	select {
	case response := <-responseChan:
		return response, nil
	case <-time.After(5 * time.Second):
		return nil, net.ErrClosed
	}
}

func (s *MessageServer) closeConnection(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	conn.Close()
	delete(s.connections, conn)
	s.logger.Info("Connection closed:", conn.RemoteAddr())
}
