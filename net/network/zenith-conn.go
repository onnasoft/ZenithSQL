package network

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/onnasoft/ZenithSQL/io/transport"
	"github.com/sirupsen/logrus"
)

type ZenithConnection struct {
	net.Conn
	responseMap map[string]chan *transport.Message
	mu          sync.Mutex
	logger      *logrus.Logger
	timeout     time.Duration
}

func NewZenithConnection(conn net.Conn, logger *logrus.Logger, timeout time.Duration) *ZenithConnection {
	connection := &ZenithConnection{
		Conn:        conn,
		responseMap: make(map[string]chan *transport.Message),
		logger:      logger,
		timeout:     timeout,

		mu: sync.Mutex{},
	}

	return connection
}

func (c *ZenithConnection) Send(message *transport.Message) (*transport.Message, error) {
	messageID := message.Header.MessageIDString()
	responseChan := make(chan *transport.Message, 1)
	defer close(responseChan)

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
		return response, nil
	case <-time.After(c.timeout):
		c.mu.Lock()
		delete(c.responseMap, messageID)
		c.mu.Unlock()
		return nil, errors.New("timeout waiting for response")
	}
}

func (c *ZenithConnection) Listen(onMessage func(*transport.Message), onClose func(error)) {
	go func() {
		for {
			message := new(transport.Message)
			err := message.ReadFrom(c.Conn)
			if err != nil {
				c.Close()
				if onClose != nil {
					onClose(err)
				}
				return
			}

			messageID := message.Header.MessageIDString()

			c.mu.Lock()
			if responseChan, exists := c.responseMap[messageID]; exists {
				delete(c.responseMap, messageID)
				c.mu.Unlock()
				responseChan <- message
			} else {
				onMessage(message)
				c.mu.Unlock()
			}
		}
	}()
}

func (c *ZenithConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.Conn.Close()
}

func DialTimeout(network, address string, logger *logrus.Logger, timeout time.Duration) (*ZenithConnection, error) {
	conn, err := net.DialTimeout(network, address, timeout)
	if err != nil {
		return nil, err
	}

	return NewZenithConnection(conn, logger, timeout), nil
}
