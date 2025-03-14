package server

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/onnasoft/ZenithSQL/nodes"
	"github.com/onnasoft/ZenithSQL/statement"
	"github.com/onnasoft/ZenithSQL/transport"
	"github.com/onnasoft/ZenithSQL/utils"
	"github.com/sirupsen/logrus"
)

type MessageServer struct {
	listener       net.Listener
	nodeManager    *nodes.NodeManager
	port           int
	logger         *logrus.Logger
	messageHandler func(net.Conn, *transport.Message)
	loginValidator func(*statement.LoginStatement) bool
	tlsConfig      *tls.Config
	timeout        time.Duration
}

func NewMessageServer(cfg *ServerConfig) *MessageServer {
	defer utils.RecoverFromPanic("NewMessageServer", cfg.Logger)

	if cfg.Port == 0 {
		cfg.Port = 8080
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}

	svr := &MessageServer{
		port:           cfg.Port,
		logger:         cfg.Logger,
		messageHandler: cfg.Handler,
		loginValidator: cfg.LoginValidator,
		nodeManager:    nodes.NewNodeManager(cfg.Logger),
		tlsConfig:      loadTLSConfig(cfg),
		timeout:        cfg.Timeout,
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

	if s.tlsConfig != nil {
		listener, err = tls.Listen("tcp", fmt.Sprintf(":%d", s.port), s.tlsConfig)
	} else {
		listener, err = net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	}

	if err != nil {
		return err
	}

	s.logger.Info("Server is running at port ", s.port)
	s.listener = listener

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		go s.handleConnection(conn)
	}
}

func (s *MessageServer) registerNode(stmt *statement.LoginStatement, conn net.Conn) {
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

	if s.listener != nil {
		return s.listener.Close()
	}

	return nil
}

func (s *MessageServer) GetRandomNode() *nodes.Node {
	return s.nodeManager.GetRandomNode()
}
