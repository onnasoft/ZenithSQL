package client

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/onnasoft/sql-parser/protocol"
	"github.com/onnasoft/sql-parser/statement"
	"github.com/onnasoft/sql-parser/transport"
	"github.com/sirupsen/logrus"
)

type MessageClient struct {
	serverAddr  string
	token       string
	conn        net.Conn
	logger      *logrus.Logger
	responseMap map[string]chan *transport.Message
	mu          sync.Mutex
}

func NewMessageClient(serverAddr, token string, logger *logrus.Logger) *MessageClient {
	return &MessageClient{
		serverAddr:  serverAddr,
		token:       token,
		logger:      logger,
		responseMap: make(map[string]chan *transport.Message),
	}
}

func (c *MessageClient) Connect() error {
	c.logger.Info("Connecting to server at:", c.serverAddr)

	conn, err := net.Dial("tcp", c.serverAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	c.logger.Info("Connected to server. Authenticating...")

	go c.listenForMessages()
	go c.sendPingPeriodically()

	if err := c.authenticate(); err != nil {
		c.logger.Error("Authentication failed:", err)
		c.conn.Close()
		return err
	}

	c.logger.Info("Authenticated successfully!")
	return nil
}

func (c *MessageClient) authenticate() error {
	timestamp := time.Now().Unix()
	stmt, _ := statement.NewLoginStatement(timestamp, c.token)
	loginMessage, _ := transport.NewMessage(protocol.Login, stmt)
	response, err := c.SendMessage(loginMessage)
	if err != nil {
		return err
	}

	if response.Header.MessageType != protocol.Login {
		return errors.New("unexpected response from server")
	}
	fmt.Println("response:", response.Stmt)

	return nil
}

func (c *MessageClient) sendPingPeriodically() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C

		c.mu.Lock()
		if c.conn == nil {
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()

		pingMessage, _ := transport.NewMessage(protocol.Ping, statement.NewEmptyStatement(protocol.Ping))
		response, err := c.SendMessage(pingMessage)

		if err != nil || response.Header.MessageType != protocol.Pong {
			c.logger.Warn("Ping failed, attempting to reconnect...")
			c.reconnect()
			return
		}

		c.logger.Info("Received Pong from server, connection is stable.")
	}
}

func (c *MessageClient) SendMessage(message *transport.Message) (*transport.Message, error) {
	c.mu.Lock()
	if c.conn == nil {
		c.mu.Unlock()
		return nil, errors.New("not connected to server")
	}

	messageID := hex.EncodeToString(message.Header.MessageID[:])
	responseChan := make(chan *transport.Message, 1)
	c.responseMap[messageID] = responseChan
	c.mu.Unlock()

	_, err := c.conn.Write(message.Serialize())
	if err != nil {
		c.mu.Lock()
		delete(c.responseMap, messageID)
		c.mu.Unlock()
		return nil, fmt.Errorf("failed to send message: %w", err)
	}

	select {
	case response := <-responseChan:
		c.mu.Lock()
		delete(c.responseMap, messageID)
		c.mu.Unlock()
		return response, nil
	case <-time.After(30 * time.Second):
		c.mu.Lock()
		delete(c.responseMap, messageID)
		c.mu.Unlock()
		return nil, errors.New("timeout waiting for response")
	}
}

func (c *MessageClient) listenForMessages() {
	for {
		header, body, err := c.readMessage(c.conn)
		if err != nil {
			c.logger.Error("Error reading message:", err)
			return
		}

		message, err := transport.ParseStatement(header, body)
		if err != nil {
			c.logger.Error("Error parsing message:", err)
			return
		}

		messageID := hex.EncodeToString(message.Header.MessageID[:])

		c.mu.Lock()
		if responseChan, exists := c.responseMap[messageID]; exists {
			responseChan <- message
		} else {
			c.logger.Warn("Received unexpected message:", string(message.Serialize()))
		}
		c.mu.Unlock()
	}
}

func (s *MessageClient) readMessage(conn net.Conn) (*transport.MessageHeader, []byte, error) {
	headerBytes := make([]byte, transport.MessageHeaderSize)
	if _, err := conn.Read(headerBytes); err != nil {
		return nil, nil, err
	}

	header := &transport.MessageHeader{}
	if err := header.Deserialize(headerBytes); err != nil {
		return nil, nil, err
	}

	body := make([]byte, header.BodySize)
	if _, err := conn.Read(body); err != nil {
		return nil, nil, err
	}

	return header, body, nil
}

func (c *MessageClient) reconnect() {
	c.logger.Warn("Attempting to reconnect...")

	for {
		time.Sleep(3 * time.Second)

		err := c.Connect()
		if err == nil {
			c.logger.Info("Reconnected successfully!")
			return
		}

		c.logger.Warn("Reconnection failed, retrying...")
	}
}

func (c *MessageClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}
