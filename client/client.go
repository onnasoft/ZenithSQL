package client

import (
	"errors"
	"sync"
	"time"

	"github.com/onnasoft/ZenithSQL/network"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/onnasoft/ZenithSQL/statement"
	"github.com/onnasoft/ZenithSQL/transport"
	"github.com/sirupsen/logrus"
)

type ConnectionPool struct {
	conn      *network.ZenithConnection
	loanCount int
}

type MessageClient struct {
	serverAddr  string
	token       string
	nodeID      string
	tags        []string
	logger      *logrus.Logger
	connections []*ConnectionPool
	mu          sync.Mutex
	maxConn     int
	minConn     int
	timeout     time.Duration
	lastUsed    int
}

type MessageConfig struct {
	ServerAddr string
	Token      string
	NodeID     string
	Tags       []string
	Logger     *logrus.Logger
	MinConn    int
	MaxConn    int
	Timeout    time.Duration
}

func NewMessageClient(config *MessageConfig) *MessageClient {
	minConn, maxConn := config.MinConn, config.MaxConn
	if maxConn <= 0 {
		maxConn = 1
	}
	if minConn <= 0 {
		minConn = 1
	}
	if minConn > maxConn {
		minConn = maxConn
	}

	client := &MessageClient{
		serverAddr:  config.ServerAddr,
		token:       config.Token,
		nodeID:      config.NodeID,
		tags:        config.Tags,
		logger:      config.Logger,
		connections: make([]*ConnectionPool, 0, maxConn),
		minConn:     minConn,
		maxConn:     maxConn,
		timeout:     config.Timeout,
		lastUsed:    -1,
	}

	client.initConnections()
	return client
}

func (c *MessageClient) initConnections() {
	for i := 0; i < c.minConn; i++ {
		conn, err := c.createConnection()
		if err == nil {
			c.connections = append(c.connections, &ConnectionPool{conn: conn, loanCount: 0})
		} else {
			c.logger.Warn("Failed to pre-create connection:", err)
		}
	}
}

func (c *MessageClient) createConnection() (*network.ZenithConnection, error) {
	conn, err := network.DialTimeout("tcp", c.serverAddr, c.logger, c.timeout)
	if err != nil {
		return nil, err
	}
	go conn.Listen()

	if err := c.authenticate(conn); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func (c *MessageClient) authenticate(conn *network.ZenithConnection) error {
	stmt, _ := statement.NewLoginStatement(c.token, c.nodeID, c.nodeID, false, c.tags)
	loginMessage, _ := transport.NewMessage(protocol.Login, stmt)
	response, err := conn.Send(loginMessage)
	if err != nil || response.Header.MessageType != protocol.Login {
		return errors.New("authentication failed")
	}
	return nil
}

func (c *MessageClient) AllocateConnection() (*network.ZenithConnection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.connections) < c.minConn {
		for i := len(c.connections); i < c.minConn; i++ {
			conn, err := c.createConnection()
			if err == nil {
				c.connections = append(c.connections, &ConnectionPool{conn: conn, loanCount: 0})
			}
		}
	}

	if len(c.connections) == 0 && len(c.connections) < c.maxConn {
		conn, err := c.createConnection()
		if err != nil {
			return nil, err
		}
		cp := &ConnectionPool{conn: conn, loanCount: 0}
		c.connections = append(c.connections, cp)
	}

	if len(c.connections) == 0 {
		return nil, errors.New("no available connections")
	}

	c.lastUsed = (c.lastUsed + 1) % len(c.connections)
	c.connections[c.lastUsed].loanCount++
	return c.connections[c.lastUsed].conn, nil
}

func (c *MessageClient) FreeConnection(conn *network.ZenithConnection) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cp := range c.connections {
		if cp.conn == conn {
			cp.loanCount--
			break
		}
	}

	if len(c.connections) > c.minConn {
		c.cleanupIdleConnections()
	}
}

func (c *MessageClient) cleanupIdleConnections() {
	for i := len(c.connections) - 1; i >= 0; i-- {
		if c.connections[i].loanCount == 0 && len(c.connections) > c.minConn {
			c.connections[i].conn.Close()
			c.connections = append(c.connections[:i], c.connections[i+1:]...)
		}
	}
}
