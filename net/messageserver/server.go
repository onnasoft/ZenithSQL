package messageserver

import (
	"crypto/tls"
	"net"
	"strings"
	"time"

	"github.com/onnasoft/ZenithSQL/core/utils"
	"github.com/onnasoft/ZenithSQL/io/response"
	"github.com/onnasoft/ZenithSQL/io/statement"
	"github.com/onnasoft/ZenithSQL/io/transport"
	"github.com/onnasoft/ZenithSQL/net/network"
	"github.com/onnasoft/ZenithSQL/net/nodes"
	"github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp/reuseport"
)

type MessageServer struct {
	listener      net.Listener
	nodeManager   *nodes.NodeManager
	address       string
	logger        *logrus.Logger
	joinValidator func(*statement.JoinClusterStatement) bool
	tlsConfig     *tls.Config
	timeout       time.Duration

	onListening  func()
	onConnection func(*network.ZenithConnection, *statement.JoinClusterStatement)
	onRequest    func(*network.ZenithConnection, *transport.MessageHeader, statement.Statement)
	onResponse   func(*network.ZenithConnection, *transport.MessageHeader, response.Response)
	onShutdown   func()
}

func NewMessageServer(cfg *ServerConfig) *MessageServer {
	defer utils.RecoverFromPanic("NewMessageServer", cfg.Logger)

	if cfg.Address == "" {
		cfg.Address = ":8080"
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}

	svr := &MessageServer{
		address:       cfg.Address,
		logger:        cfg.Logger,
		joinValidator: cfg.JoinValidator,
		nodeManager:   nodes.NewNodeManager(cfg.Logger),
		tlsConfig:     loadTLSConfig(cfg),
		timeout:       cfg.Timeout,

		onListening:  cfg.OnListening,
		onConnection: cfg.OnConnection,
		onRequest:    cfg.OnRequest,
		onResponse:   cfg.OnResponse,
		onShutdown:   cfg.OnShutdown,
	}

	if cfg.Logger == nil {
		svr.logger = logrus.New()
	}

	return svr
}

func (s *MessageServer) Start() error {
	defer utils.RecoverFromPanic("Start", s.logger)
	defer s.nodeManager.ClearAllNodes()

	var listener net.Listener
	var err error

	listener, err = reuseport.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	if s.tlsConfig != nil {
		listener = tls.NewListener(listener, s.tlsConfig)
	}

	if s.onListening != nil {
		s.onListening()
	}
	s.listener = listener

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		go s.handleConnection(conn)
	}
}

func (s *MessageServer) registerNode(stmt *statement.JoinClusterStatement, conn *network.ZenithConnection) {
	defer utils.RecoverFromPanic("registerNode", s.logger)

	var role nodes.NodeRole
	if strings.Contains(stmt.NodeID, "master") {
		role = nodes.Master
	} else {
		role = nodes.Slave
	}

	node := s.nodeManager.GetNode(stmt.NodeID)
	if node == nil {
		node = s.nodeManager.AddNode(stmt, role)
	}
	node.AddConnection(conn)
}

func (s *MessageServer) Stop() error {
	defer utils.RecoverFromPanic("Stop", s.logger)

	if s.onShutdown != nil {
		s.onShutdown()
	}

	if s.listener != nil {
		return s.listener.Close()
	}

	return nil
}

func (s *MessageServer) GetRandomNode() *nodes.Node {
	return s.nodeManager.GetRandomNode()
}

func (s *MessageServer) SendToAllSlaves(msg *transport.Message) []*transport.ExecutionResult {
	return s.nodeManager.SendToAllSlaves(msg)
}

func (c *MessageServer) Addr() string {
	return c.address
}
