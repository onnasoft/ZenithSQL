package client

import (
	"container/heap"
	"errors"
	"sync"
	"time"

	"github.com/onnasoft/ZenithSQL/network"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/onnasoft/ZenithSQL/statement"
	"github.com/onnasoft/ZenithSQL/transport"
	"github.com/onnasoft/ZenithSQL/utils"
	"github.com/sirupsen/logrus"
)

type MessageClient struct {
	serverAddr  string
	token       string
	nodeID      string
	tags        []string
	logger      *logrus.Logger
	connections PriorityQueue
	mu          sync.Mutex
	maxConn     int
	minConn     int
	timeout     time.Duration
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
		connections: make(PriorityQueue, 0, maxConn),
		minConn:     minConn,
		maxConn:     maxConn,
		timeout:     config.Timeout,
	}

	heap.Init(&client.connections)
	client.initConnections()
	return client
}

func (c *MessageClient) initConnections() {
	for i := 0; i < c.minConn; i++ {
		conn, err := c.createConnection()
		if err == nil {
			heap.Push(&c.connections, &ConnectionPool{conn: conn})
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

	go conn.ListenWithCallback(func(err error) {
		c.logger.Warn("Connection lost, attempting to reconnect...")

		time.Sleep(c.timeout)
		c.handleConnectionFailure(conn)
	})

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
	defer utils.RecoverFromPanic("AllocateConnection", c.logger)
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connections.Len() == 0 && len(c.connections) < c.maxConn {
		conn, err := c.createConnection()
		if err != nil {
			return nil, err
		}
		heap.Push(&c.connections, &ConnectionPool{conn: conn})
	}

	if c.connections.Len() == 0 {
		return nil, errors.New("no available connections")
	}

	selected := heap.Pop(&c.connections).(*ConnectionPool)
	selected.loanCount++
	heap.Push(&c.connections, selected)

	return selected.conn, nil
}

func (c *MessageClient) FreeConnection(conn *network.ZenithConnection) {
	defer utils.RecoverFromPanic("FreeConnection", c.logger)

	c.mu.Lock()
	defer c.mu.Unlock()

	for i, cp := range c.connections {
		if cp.conn == conn {
			cp.loanCount--
			heap.Fix(&c.connections, i)
			break
		}
	}

	if len(c.connections) > c.minConn {
		c.cleanupIdleConnections()
	}
}

func (c *MessageClient) cleanupIdleConnections() {
	c.mu.Lock()
	defer c.mu.Unlock()

	remaining := PriorityQueue{}
	for c.connections.Len() > c.minConn {
		cp := heap.Pop(&c.connections).(*ConnectionPool)
		if cp.loanCount > 0 {
			heap.Push(&remaining, cp)
		} else {
			cp.conn.Close()
		}
	}
	c.connections = remaining
	heap.Init(&c.connections)
}

func (c *MessageClient) handleConnectionFailure(failedConn *network.ZenithConnection) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, cp := range c.connections {
		if cp.conn == failedConn {
			c.connections = append(c.connections[:i], c.connections[i+1:]...)
			break
		}
	}

	if len(c.connections) < c.minConn {
		c.logger.Info("Recreating lost connection")
		conn, err := c.createConnection()
		if err == nil {
			heap.Push(&c.connections, &ConnectionPool{conn: conn})
		} else {
			c.logger.Warn("Failed to recreate connection:", err)
		}
	}
}
