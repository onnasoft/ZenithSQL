package server

import (
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/onnasoft/ZenithSQL/statement"
	"github.com/onnasoft/ZenithSQL/transport"
	"github.com/sirupsen/logrus"
)

type MessageTask struct {
	Message    *transport.Message
	Connection *ConnectionHandler
}

type MessageServer struct {
	listener       net.Listener
	nodeManager    *NodeManager
	taskQueue      chan *MessageTask
	responseMap    map[string]chan *transport.Message
	port           int
	logger         *logrus.Logger
	messageHandler func(*ConnectionHandler, *transport.Message)
	loginValidator func(*statement.LoginStatement) bool
	tlsConfig      *tls.Config
	mu             sync.Mutex
}

func NewMessageServer(cfg *ServerConfig) *MessageServer {
	defer recoverFromPanic("NewMessageServer", nil)

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
		nodeManager:    NewNodeManager(cfg.Logger),
		taskQueue:      make(chan *MessageTask),
		responseMap:    make(map[string]chan *transport.Message),
		tlsConfig:      tlsConfig,
	}

	if cfg.Logger == nil {
		svr.logger = logrus.New()
	}

	return svr
}

func (s *MessageServer) Start() error {
	defer recoverFromPanic("Start", s)

	defer func() {
		s.mu.Lock()
		s.nodeManager.ClearAllNodes()
		close(s.taskQueue)
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

	go s.processQueue()
	s.listener = listener

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		go s.handleConnection(conn)
	}
}

func (s *MessageServer) processQueue() {
	defer recoverFromPanic("processQueue", s)

	for task := range s.taskQueue {
		_, err := s.SendMessage(task.Connection, task.Message)
		if err != nil {
			s.logger.Error("Error sending message:", err)
		}
	}
}

func (s *MessageServer) readMessage(conn net.Conn) (*transport.MessageHeader, []byte, error) {
	headerBytes := make([]byte, transport.MessageHeaderSize)
	if _, err := conn.Read(headerBytes); err != nil {
		return nil, nil, err
	}

	header := &transport.MessageHeader{}
	if err := header.Deserialize(headerBytes); err != nil {
		return nil, nil, err
	}

	body := make([]byte, header.BodySize)
	if _, err := conn.Read(body); err != nil {
		return nil, nil, err
	}

	return header, body, nil
}

func (s *MessageServer) SendToAll(message *transport.Message) []*MessageResponse {
	defer recoverFromPanic("SendToAll", s)

	s.mu.Lock()
	defer s.mu.Unlock()

	var wg sync.WaitGroup
	responseChan := make(chan *MessageResponse, s.nodeManager.CountNodes())

	wg.Add(s.nodeManager.CountNodes())
	for _, node := range s.nodeManager.GetAllNodes() {
		for conn := range node.Connections {
			go func(c *ConnectionHandler) {
				defer wg.Done()
				response, err := s.SendMessage(c, message)
				responseChan <- &MessageResponse{
					Result: response,
					Error:  err,
				}
			}(conn)
		}
	}

	wg.Wait()
	close(responseChan)

	var responses []*MessageResponse
	for response := range responseChan {
		responses = append(responses, response)
	}

	return responses
}

func (s *MessageServer) SendMessage(conn *ConnectionHandler, message *transport.Message) (*transport.Message, error) {
	defer recoverFromPanic("SendMessage", s)

	if err := s.SendSilentMessage(conn, message); err != nil {
		return nil, err
	}

	messageID := hex.EncodeToString(message.Header.MessageID[:])

	s.mu.Lock()
	responseChan := make(chan *transport.Message, 1)
	s.responseMap[messageID] = responseChan
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.responseMap, messageID)
		s.mu.Unlock()
	}()

	select {
	case response := <-responseChan:
		node := s.nodeManager.GetNodeByConnection(conn)
		if node != nil {
			for _, replica := range node.Replicas {
				_ = s.SendSilentMessage(replica, message)
			}
		}
		return response, nil
	case <-time.After(5 * time.Second):
		return nil, net.ErrClosed
	}
}

func (s *MessageServer) SendSilentMessage(conn *ConnectionHandler, message *transport.Message) error {
	defer recoverFromPanic("SendSilentMessage", s)

	_, err := conn.Send(message)
	return err
}

func (s *MessageServer) Stop() error {
	defer recoverFromPanic("Stop", s)

	if s.listener == nil {
		return fmt.Errorf("server is not running")
	}
	return s.listener.Close()
}

func (s *MessageServer) closeConnection(conn *ConnectionHandler, reason string) {
	defer recoverFromPanic("closeConnection", s)

	s.mu.Lock()
	defer s.mu.Unlock()

	node := s.nodeManager.GetNodeByConnection(conn)
	if node != nil {
		node.RemoveConnection(conn)
		s.logger.Info("Connection closed:", conn.RemoteAddr(), " Reason:", reason)
	}
}
