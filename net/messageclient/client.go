package messageclient

import (
	"container/heap"
	"errors"
	"sync"
	"time"

	"github.com/onnasoft/ZenithSQL/core/utils"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/onnasoft/ZenithSQL/io/statement"
	"github.com/onnasoft/ZenithSQL/io/transport"
	"github.com/onnasoft/ZenithSQL/net/network"
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

	onConnection func()
	onMessage    func(*network.ZenithConnection, *transport.Message)
	onShutdown   func()
}

func NewMessageClient(config *MessageConfig) *MessageClient {
	defer utils.RecoverFromPanic("NewMessageClient", config.Logger)
	defer func() {
		if config.OnConnection != nil {
			config.OnConnection()
		}
	}()

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

		onConnection: config.OnConnection,
		onMessage:    config.OnMessage,
		onShutdown:   config.OnShutdown,
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

	handleConnectionFailure := func(err error) {
		c.handleConnectionFailure(conn)
	}
	handleMessage := func(m *transport.Message) {
		if c.onMessage != nil {
			c.onMessage(conn, m)
		}
	}

	go conn.Listen(handleMessage, handleConnectionFailure)

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

func (c *MessageClient) Shutdown() {
	defer utils.RecoverFromPanic("Shutdown", c.logger)
	defer func() {
		if c.onShutdown != nil {
			c.onShutdown()
		}
	}()

	c.mu.Lock()
	for _, cp := range c.connections {
		cp.conn.Close()
	}
	c.mu.Unlock()
}

func (c *MessageClient) ServerAddr() string {
	return c.serverAddr
}
