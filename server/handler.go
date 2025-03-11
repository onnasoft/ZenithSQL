package server

import (
	"encoding/hex"
	"log"
	"net"
	"time"

	"github.com/onnasoft/sql-parser/protocol"
	"github.com/onnasoft/sql-parser/transport"
)

func (s *TCPServer) handleConnection(conn net.Conn) {
	s.mu.Lock()
	s.conns[conn] = struct{}{}
	s.mu.Unlock()

	log.Println("New connection:", conn.RemoteAddr())

	go s.reader(conn)
	go s.alive(conn)

	_, err := s.SendMessage(conn, transport.NewMessage(protocol.Welcome, []byte("Welcome to the server!")))
	if err != nil {
		log.Println("Error sending welcome message:", err)
	}
}

func (s *TCPServer) alive(conn net.Conn) {
	for {
		_, err := s.SendMessage(conn, transport.NewMessage(protocol.Ping, []byte("Are you alive?")))
		if err != nil {
			log.Println("Error sending alive message:", err)
			return
		}

		time.Sleep(30 * time.Second)
	}
}

func (s *TCPServer) reader(conn net.Conn) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.logger.Info("Connection closed:", conn.RemoteAddr())
	}()

	for {
		headerBytes := make([]byte, 24)
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

		messageId := hex.EncodeToString(message.Header.MessageID[:])

		s.mu.Lock()
		if responseChan, ok := s.responses[messageId]; ok {
			responseChan <- message
			delete(s.responses, messageId)
		} else if s.handler != nil {
			go s.handler(message)
		}
		s.mu.Unlock()
	}
}

func (s *TCPServer) SendMessage(conn net.Conn, message *transport.Message) (*transport.Message, error) {
	_, err := conn.Write(message.Serialize())
	if err != nil {
		return nil, err
	}

	messageId := hex.EncodeToString(message.Header.MessageID[:])

	s.mu.Lock()
	s.responses[messageId] = make(chan *transport.Message)
	s.mu.Unlock()

	response := <-s.responses[messageId]

	s.mu.Lock()
	delete(s.responses, messageId)
	s.mu.Unlock()

	return response, nil
}
