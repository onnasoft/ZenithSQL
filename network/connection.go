package network

import (
	"net"
	"sync"

	"github.com/sirupsen/logrus"
)

type Connection struct {
	net.Conn
	NodeID    string
	sendQueue chan []byte
	closeChan chan struct{}
	mu        sync.Mutex
	logger    *logrus.Logger
}

func NewConnection(conn net.Conn, logger *logrus.Logger) *Connection {
	c := &Connection{
		Conn:      conn,
		sendQueue: make(chan []byte, 100),
		closeChan: make(chan struct{}),
		logger:    logger,
	}

	go c.startWriter()
	return c
}

func NewNodeConnection(conn net.Conn, nodeID string, logger *logrus.Logger) *Connection {
	c := NewConnection(conn, logger)
	c.NodeID = nodeID

	go c.startWriter()
	return c
}

func (c *Connection) Write(data []byte) (int, error) {
	select {
	case c.sendQueue <- data:
		return len(data), nil
	case <-c.closeChan:
		return 0, net.ErrClosed
	}
}

func (c *Connection) startWriter() {
	for {
		select {
		case data := <-c.sendQueue:
			if data == nil {
				continue
			}
			_, err := c.Conn.Write(data)
			if err != nil {
				c.logger.Error("Failed to write to", c.RemoteAddr(), ":", err)
				c.Close()
				return
			}
		case <-c.closeChan:
			return
		}
	}
}

func (c *Connection) Close() error {
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
