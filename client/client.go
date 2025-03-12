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

type Connection struct {
	conn      net.Conn
	active    bool
	closeChan chan struct{}
}

type MessageClient struct {
	serverAddr  string
	token       string
	nodeID      string
	tags        []string
	logger      *logrus.Logger
	responseMap map[string]chan *transport.Message
	connections []*Connection
	mu          sync.Mutex
	maxConn     int
	lastUsed    int
	wg          sync.WaitGroup
	timeout     time.Duration
}

type MessageConfig struct {
	ServerAddr string
	Token      string
	NodeID     string
	Tags       []string
	MaxConn    int
	Timeout    time.Duration
	Logger     *logrus.Logger
}

func NewMessageClient(config *MessageConfig) *MessageClient {
	return &MessageClient{
		serverAddr:  config.ServerAddr,
		token:       config.Token,
		nodeID:      config.NodeID,
		tags:        config.Tags,
		logger:      config.Logger,
		responseMap: make(map[string]chan *transport.Message),
		maxConn:     config.MaxConn,
		lastUsed:    -1,
		timeout:     config.Timeout,
	}
}

func (c *MessageClient) Connect() error {
	c.logger.Info("Connecting to server at:", c.serverAddr)

	done := make(chan struct{})
	c.wg.Add(c.maxConn)

	go func() {
		for i := 0; i < c.maxConn; i++ {
			go c.startConnection()
		}
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(c.timeout):
		return errors.New("connection timeout exceeded")
	}
}

func (c *MessageClient) startConnection() {
	defer c.wg.Done()

	for {
		conn, err := net.DialTimeout("tcp", c.serverAddr, c.timeout)
		if err != nil {
			c.logger.Warn("Failed to connect to server, retrying in 3s...")
			time.Sleep(3 * time.Second)
			continue
		}

		newConn := &Connection{
			conn:      conn,
			active:    true,
			closeChan: make(chan struct{}),
		}

		c.mu.Lock()
		c.connections = append(c.connections, newConn)
		c.mu.Unlock()

		go c.listenForMessages(newConn)
		go c.sendPingPeriodically(newConn)

		if err := c.authenticate(newConn); err != nil {
			c.logger.Error("Authentication failed, retrying...")
			newConn.conn.Close()
			newConn.active = false
			time.Sleep(3 * time.Second)
			continue
		}

		c.logger.Info("Authenticated successfully on new connection")
		break
	}
}

func (c *MessageClient) authenticate(conn *Connection) error {
	stmt, _ := statement.NewLoginStatement(c.token, c.nodeID, c.nodeID, false, c.tags)
	loginMessage, _ := transport.NewMessage(protocol.Login, stmt)
	response, err := c.sendMessage(conn, loginMessage)
	if err != nil {
		return err
	}

	if response.Header.MessageType != protocol.Login {
		return errors.New("unexpected response from server")
	}

	return nil
}

func (c *MessageClient) sendPingPeriodically(conn *Connection) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !conn.active {
				return
			}

			pingMessage, _ := transport.NewMessage(protocol.Ping, statement.NewEmptyStatement(protocol.Ping))
			response, err := c.SendMessage(pingMessage)

			if err != nil || response.Header.MessageType != protocol.Pong {
				c.logger.Warn("Ping failed, marking connection inactive...")
				conn.active = false
				c.reconnect(conn)
				return
			}
		case <-conn.closeChan:
			return
		}
	}
}

func (c *MessageClient) sendMessage(conn *Connection, message *transport.Message) (*transport.Message, error) {
	messageID := hex.EncodeToString(message.Header.MessageID[:])
	responseChan := make(chan *transport.Message, 1)

	c.mu.Lock()
	c.responseMap[messageID] = responseChan
	c.mu.Unlock()

	_, err := conn.conn.Write(message.Serialize())
	if err != nil {
		delete(c.responseMap, messageID)
		return nil, fmt.Errorf("failed to send message: %w", err)
	}

	select {
	case response := <-responseChan:
		c.mu.Lock()
		delete(c.responseMap, messageID)
		c.mu.Unlock()
		return response, nil
	case <-time.After(c.timeout):
		c.mu.Lock()
		delete(c.responseMap, messageID)
		c.mu.Unlock()
		return nil, errors.New("timeout waiting for response")
	}
}

func (c *MessageClient) SendMessage(message *transport.Message) (*transport.Message, error) {
	c.mu.Lock()
	conn := c.getAvailableConnection()
	c.mu.Unlock()

	if conn == nil {
		c.logger.Warn("No active connections available, trying to reconnect...")
		return nil, errors.New("no active connections available")
	}

	return c.sendMessage(conn, message)
}

func (c *MessageClient) SendMessageToAll(message *transport.Message) []*transport.Message {
	wg := sync.WaitGroup{}
	wg.Add(len(c.connections))
	messages := make([]*transport.Message, len(c.connections))
	mu := sync.Mutex{}

	for _, conn := range c.connections {
		go func(conn *Connection) {
			defer wg.Done()
			response, err := c.sendMessage(conn, message)
			if err != nil {
				c.logger.Error("Failed to send message:", err)
				return
			}

			mu.Lock()
			messages = append(messages, response)
			mu.Unlock()
		}(conn)
	}
	wg.Wait()

	return messages
}

func (c *MessageClient) getAvailableConnection() *Connection {
	activeConnections := make([]*Connection, 0)
	for _, conn := range c.connections {
		if conn.active {
			activeConnections = append(activeConnections, conn)
		}
	}

	if len(activeConnections) == 0 {
		return nil
	}

	c.lastUsed = (c.lastUsed + 1) % len(activeConnections)
	return activeConnections[c.lastUsed]
}

func (c *MessageClient) listenForMessages(conn *Connection) {
	for {
		header, body, err := c.readMessage(conn.conn)
		if err != nil {
			c.logger.Error("Error reading message: ", err)
			conn.active = false
			return
		}

		message, err := transport.ParseStatement(header, body)
		if err != nil {
			c.logger.Error("Error parsing message: ", err)
			conn.active = false
			return
		}

		messageID := hex.EncodeToString(message.Header.MessageID[:])

		c.mu.Lock()
		if responseChan, exists := c.responseMap[messageID]; exists {
			responseChan <- message
		} else {
			c.logger.Warn("Received unexpected message:", message.Header.MessageType)
		}
		c.mu.Unlock()
	}
}

func (c *MessageClient) readMessage(conn net.Conn) (*transport.MessageHeader, []byte, error) {
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

func (c *MessageClient) reconnect(conn *Connection) {
	conn.conn.Close()
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, s := range c.connections {
		if s == conn {
			c.connections = append(c.connections[:i], c.connections[i+1:]...)
			break
		}
	}

	go c.startConnection()
}
