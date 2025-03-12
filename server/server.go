package server

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/onnasoft/sql-parser/transport"
	"github.com/sirupsen/logrus"
)

type MessageTask struct {
	Message    *transport.Message
	Connection net.Conn
}

type MessageServer struct {
	listener       net.Listener
	connections    map[net.Conn]struct{}
	taskQueue      chan *MessageTask
	responseMap    map[string]chan *transport.Message
	port           int
	logger         *logrus.Logger
	messageHandler func(net.Conn, *transport.Message)
	mu             sync.Mutex
}

type ServerConfig struct {
	Port    int
	Handler func(net.Conn, *transport.Message)
	Logger  *logrus.Logger
}

func NewMessageServer(cfg *ServerConfig) *MessageServer {
	return &MessageServer{
		port:           cfg.Port,
		logger:         cfg.Logger,
		messageHandler: cfg.Handler,
		connections:    make(map[net.Conn]struct{}),
		taskQueue:      make(chan *MessageTask),
		responseMap:    make(map[string]chan *transport.Message),
	}
}

func (s *MessageServer) Start() error {
	defer func() {
		for conn := range s.connections {
			conn.Close()
		}
		s.connections = make(map[net.Conn]struct{})
		close(s.taskQueue)
	}()

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
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
	for task := range s.taskQueue {
		response, err := s.SendMessage(task.Connection, task.Message)
		if err != nil {
			s.logger.Error("Error sending message:", err)
		}
		s.logger.Info("Response: ", response)
	}
}

type MessageResponse struct {
	Result *transport.Message
	Error  error
}

func (s *MessageServer) SendToAll(message *transport.Message) []*MessageResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	var wg sync.WaitGroup
	responseChan := make(chan *MessageResponse, len(s.connections))

	wg.Add(len(s.connections))
	for conn := range s.connections {
		go func(c net.Conn) {
			defer wg.Done()
			response, err := s.SendMessage(c, message)
			responseChan <- &MessageResponse{
				Result: response,
				Error:  err,
			}
		}(conn)
	}

	wg.Wait()
	close(responseChan)

	responses := make([]*MessageResponse, 0, len(s.connections))
	for response := range responseChan {
		responses = append(responses, response)
	}

	return responses
}

func (s *MessageServer) Stop() error {
	if s.listener == nil {
		return fmt.Errorf("server is not running")
	}
	return s.listener.Close()
}

func (s *MessageServer) GetRandomConnection() net.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.connections) == 0 {
		return nil
	}

	connections := make([]net.Conn, 0, len(s.connections))
	for conn := range s.connections {
		connections = append(connections, conn)
	}

	h := rand.New(rand.NewSource(time.Now().UnixNano()))
	return connections[h.Intn(len(connections))]
}
