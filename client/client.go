package client

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/onnasoft/ZenithSQL/network"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/onnasoft/ZenithSQL/statement"
	"github.com/onnasoft/ZenithSQL/transport"
	"github.com/sirupsen/logrus"
)

type MessageClient struct {
	serverAddr  string
	token       string
	nodeID      string
	tags        []string
	logger      *logrus.Logger
	responseMap map[string]chan *transport.Message
	connections []net.Conn
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
	maxConn := config.MaxConn
	if maxConn <= 0 {
		maxConn = 1
	}
	return &MessageClient{
		serverAddr:  config.ServerAddr,
		token:       config.Token,
		nodeID:      config.NodeID,
		tags:        config.Tags,
		logger:      config.Logger,
		responseMap: make(map[string]chan *transport.Message),
		maxConn:     maxConn,
		lastUsed:    -1,
		timeout:     config.Timeout,
	}
}

func (c *MessageClient) Connect() error {
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

		nconn := network.NewConnection(conn, c.logger)
		c.mu.Lock()
		c.connections = append(c.connections, nconn)
		c.mu.Unlock()

		go c.listenForMessages(nconn)
		go c.managePings(nconn)

		if err := c.authenticate(nconn); err != nil {
			c.logger.Error("Failed to authenticate connection: ", err)
			c.closeConnection(nconn)
			time.Sleep(3 * time.Second)
			continue
		}

		break
	}
}

func (c *MessageClient) authenticate(conn net.Conn) error {
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

func (c *MessageClient) managePings(conn net.Conn) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pingMessage, _ := transport.NewMessage(protocol.Ping, statement.NewEmptyStatement(protocol.Ping))
			response, err := c.SendMessage(pingMessage)

			if err != nil || response.Header.MessageType != protocol.Pong {
				c.logger.Warn("Ping failed, marking connection inactive...")
				c.closeConnection(conn)
				c.reconnect(conn)
				return
			}

		case <-c.closeChan(conn):
			return
		}
	}
}

func (c *MessageClient) sendMessage(conn net.Conn, message *transport.Message) (*transport.Message, error) {
	messageID := hex.EncodeToString(message.Header.MessageID[:])
	responseChan := make(chan *transport.Message, 1)

	c.mu.Lock()
	c.responseMap[messageID] = responseChan
	c.mu.Unlock()

	_, err := conn.Write(message.ToBytes())
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
	case <-time.After(c.timeout):
		c.mu.Lock()
		delete(c.responseMap, messageID)
		c.mu.Unlock()
		return nil, errors.New("timeout waiting for response")
	}
}

func (c *MessageClient) SendMessage(message *transport.Message) (*transport.Message, error) {
	conn := c.getAvailableConnection()

	if conn == nil {
		c.logger.Warn("No active connections available, trying to reconnect...")
		return nil, errors.New("no active connections available")
	}

	return c.sendMessage(conn, message)
}

func (c *MessageClient) getAvailableConnection() net.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, conn := range c.connections {
		return conn
	}

	return nil
}

func (c *MessageClient) listenForMessages(conn net.Conn) {
	for {
		message := new(transport.Message)
		err := message.ReadFrom(conn)
		if err != nil {
			c.logger.Error("Error parsing message: ", err)
			c.closeConnection(conn)
			return
		}

		c.logger.Debug("Received message:", message.Header.MessageType)

		messageID := hex.EncodeToString(message.Header.MessageID[:])

		c.logger.Debug("Received message:", message.Header.MessageType)
		c.logger.Debug("Message ID:", messageID)
		c.logger.Debug("Message Body:", message.Stmt)

		c.mu.Lock()
		if responseChan, exists := c.responseMap[messageID]; exists {
			responseChan <- message
		} else {
			c.logger.Warn("Received unexpected message:", message.Header.MessageType)
		}
		c.mu.Unlock()
	}
}

func (c *MessageClient) closeConnection(conn net.Conn) {
	conn.Close()

	c.mu.Lock()
	for i, s := range c.connections {
		if s == conn {
			c.connections = append(c.connections[:i], c.connections[i+1:]...)
			break
		}
	}
	c.mu.Unlock()
}

func (c *MessageClient) reconnect(conn net.Conn) {
	c.closeConnection(conn)
	go c.startConnection()
}

func (c *MessageClient) closeChan(conn net.Conn) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, c := range c.connections {
			if c == conn {
				return
			}
		}
		close(ch)
	}()
	return ch
}
