package network

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/onnasoft/ZenithSQL/transport"
	"github.com/sirupsen/logrus"
)

type ZenithConnection struct {
	net.Conn
	ID          string
	responseMap map[string]chan *transport.Message
	mu          sync.Mutex
	logger      *logrus.Logger
	timeout     time.Duration

	sendQueue chan []byte
	closeChan chan struct{}
}

func NewZenithConnection(conn net.Conn, logger *logrus.Logger, timeout time.Duration) *ZenithConnection {
	connection := &ZenithConnection{
		Conn:        conn,
		responseMap: make(map[string]chan *transport.Message),
		logger:      logger,
		timeout:     timeout,

		mu:        sync.Mutex{},
		sendQueue: make(chan []byte, 100),
		closeChan: make(chan struct{}),
	}

	go connection.startWriter()
	return connection
}

func (c *ZenithConnection) Send(message *transport.Message) (*transport.Message, error) {
	messageID := message.Header.MessageIDString()
	responseChan := make(chan *transport.Message, 1)

	c.mu.Lock()
	c.responseMap[messageID] = responseChan
	c.mu.Unlock()

	_, err := c.Conn.Write(message.ToBytes())
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

func (c *ZenithConnection) startWriter() {
	for {
		select {
		case data := <-c.sendQueue:
			if data == nil {
				continue
			}
			_, err := c.Conn.Write(data)
			if err != nil {
				c.logger.Error("Failed to write to", c.Conn.RemoteAddr(), ":", err)
				c.Close()
				return
			}
		case <-c.closeChan:
			return
		}
	}
}

func (c *ZenithConnection) Write(data []byte) (int, error) {
	select {
	case c.sendQueue <- data:
		return len(data), nil
	case <-c.closeChan:
		return 0, net.ErrClosed
	}
}

func (c *ZenithConnection) Listen() {
	for {
		message := new(transport.Message)
		err := message.ReadFrom(c.Conn)
		if err != nil {
			c.logger.Error("Error reading message: ", err)
			c.Close()
			return
		}

		messageID := message.Header.MessageIDString()

		c.mu.Lock()
		if responseChan, exists := c.responseMap[messageID]; exists {
			responseChan <- message
		} else {
			c.logger.Warn("Received unexpected message:", message.Header.MessageType)
		}
		c.mu.Unlock()
	}
}

func (c *ZenithConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	select {
	case <-c.closeChan:
		return nil
	default:
		close(c.closeChan)
		close(c.sendQueue)
		return c.Conn.Close()
	}
}

func DialTimeout(network, address string, logger *logrus.Logger, timeout time.Duration) (*ZenithConnection, error) {
	conn, err := net.DialTimeout(network, address, timeout)
	if err != nil {
		return nil, err
	}

	return NewZenithConnection(conn, logger, timeout), nil
}
