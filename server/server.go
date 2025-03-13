package server

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/onnasoft/ZenithSQL/nodes"
	"github.com/onnasoft/ZenithSQL/statement"
	"github.com/onnasoft/ZenithSQL/transport"
	"github.com/onnasoft/ZenithSQL/utils"
	"github.com/sirupsen/logrus"
)

type MessageServer struct {
	listener       net.Listener
	nodeManager    *nodes.NodeManager
	responseMap    map[string]chan *transport.Message
	port           int
	logger         *logrus.Logger
	messageHandler func(net.Conn, *transport.Message)
	loginValidator func(*statement.LoginStatement) bool
	tlsConfig      *tls.Config
	mu             sync.Mutex
}

func NewMessageServer(cfg *ServerConfig) *MessageServer {
	defer utils.RecoverFromPanic("NewMessageServer", cfg.Logger)

	var tlsConfig *tls.Config
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			cfg.Logger.Fatal("Failed to load TLS certificate:", err)
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}

	svr := &MessageServer{
		port:           cfg.Port,
		logger:         cfg.Logger,
		messageHandler: cfg.Handler,
		loginValidator: cfg.LoginValidator,
		nodeManager:    nodes.NewNodeManager(cfg.Logger),
		responseMap:    make(map[string]chan *transport.Message),
		tlsConfig:      tlsConfig,
	}

	if cfg.Logger == nil {
		svr.logger = logrus.New()
	}

	return svr
}

func (s *MessageServer) Start() error {
	defer utils.RecoverFromPanic("Start", s.logger)

	defer func() {
		s.mu.Lock()
		s.nodeManager.ClearAllNodes()
		s.mu.Unlock()
	}()

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

func (s *MessageServer) registerNode(nodeID string, conn net.Conn) {
	defer utils.RecoverFromPanic("registerNode", s.logger)

	var role nodes.NodeRole
	if strings.Contains(nodeID, "master") {
		role = nodes.Master
	} else {
		role = nodes.Slave
	}

	node := s.nodeManager.GetNode(nodeID)
	if node == nil {
		node = s.nodeManager.AddNode(nodeID, role)
	}
	node.AddConnection(conn)
}
