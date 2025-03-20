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

const reconnectInterval = 3 * time.Second

type MessageClient struct {
	serverAddr         string
	token              string
	nodeID             string
	tags               []string
	logger             *logrus.Logger
	connections        PriorityQueue
	mu                 sync.Mutex
	maxConn            int
	minConn            int
	pendingConnections int
	timeout            time.Duration
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
	wg := sync.WaitGroup{}
	wg.Add(c.minConn)

	for i := 0; i < c.minConn; i++ {
		go func() {
			defer wg.Done()
			c.retryCreateConnection()
		}()
	}

	wg.Wait()
}

func (c *MessageClient) retryCreateConnection() {
	c.mu.Lock()
	if c.pendingConnections+c.connections.Len() >= c.maxConn {
		c.mu.Unlock()
		return
	}
	c.pendingConnections++
	c.mu.Unlock()
	for {
		conn, err := c.createConnection()
		if err == nil {
			c.mu.Lock()
			if c.connections.Len() >= c.maxConn {
				c.mu.Unlock()
				conn.Close()
				c.logger.Warn("Connection pool is full")
				return
			}
			c.pendingConnections--
			c.connections.Push(&ConnectionPool{conn: conn})
			c.mu.Unlock()
			return
		}

		time.Sleep(reconnectInterval)
	}
}

func (c *MessageClient) createConnection() (*network.ZenithConnection, error) {
	conn, err := network.DialTimeout("tcp", c.serverAddr, c.logger, c.timeout)
	if err != nil {
		return nil, err
	}

	go conn.ListenWithCallback(func(err error) {
		c.handleConnectionFailure(conn)
	})

	if err := c.authenticate(conn); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func (c *MessageClient) authenticate(conn *network.ZenithConnection) error {
	stmt, err := statement.NewJoinClusterStatement(c.token, c.nodeID, c.nodeID, false, c.tags)
	if err != nil {
		return err
	}
	loginMessage, _ := transport.NewMessage(protocol.Login, stmt)
	response, err := conn.Send(loginMessage)
	if err != nil || response.Header.MessageType != protocol.Login {
		return errors.New("authentication failed")
	}
	return nil
}

func (c *MessageClient) AllocateConnection() (*network.ZenithConnection, error) {
	defer utils.RecoverFromPanic("AllocateConnection", c.logger)

	if c.connections.Len() == 0 || c.connections.Len() < c.maxConn {
		c.retryCreateConnection()
	}

	c.mu.Lock()
	if c.connections.Len() == 0 {
		return nil, errors.New("no available connections")
	}

	selected := c.connections[0]
	c.connections = append(c.connections, selected)[1:]

	selected.loanCount++
	c.mu.Unlock()

	return selected.conn, nil
}

func (c *MessageClient) FreeConnection(conn *network.ZenithConnection) {
	defer utils.RecoverFromPanic("FreeConnection", c.logger)

	c.mu.Lock()
	for i, cp := range c.connections {
		if cp.conn == conn {
			cp.loanCount--
			if cp.loanCount <= 0 {
				heap.Remove(&c.connections, i)
			} else {
				heap.Fix(&c.connections, i)
			}
			break
		}
	}
	c.mu.Unlock()
}

func (c *MessageClient) handleConnectionFailure(failedConn *network.ZenithConnection) {
	c.mu.Lock()

	for i, cp := range c.connections {
		if cp.conn == failedConn {
			c.connections = append(c.connections[:i], c.connections[i+1:]...)
			break
		}
	}
	c.mu.Unlock()

	c.retryCreateConnection()
}
