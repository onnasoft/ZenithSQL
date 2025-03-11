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

type Task struct {
	Message    *transport.Message
	Connection net.Conn
}

type TCPServer struct {
	port      int
	listener  net.Listener
	conns     map[net.Conn]struct{}
	queue     chan *Task
	responses map[string]chan *transport.Message
	logger    logrus.Logger
	handler   func(*transport.Message)
	mu        sync.Mutex
}

func NewTCPServer(port int) *TCPServer {
	return &TCPServer{
		port:      port,
		conns:     make(map[net.Conn]struct{}),
		queue:     make(chan *Task),
		responses: make(map[string]chan *transport.Message),
	}
}

func (s *TCPServer) SetHandler(handler func(*transport.Message)) {
	s.handler = handler
}

func (s *TCPServer) Start() error {
	defer func() {
		for conn := range s.conns {
			conn.Close()
		}

		s.conns = make(map[net.Conn]struct{})
		close(s.queue)
	}()
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return err
	}

	s.logger.Info("Server is running at port ", s.port)

	go s.sender()
	s.listener = listener
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		go s.handleConnection(conn)
	}
}

func (s *TCPServer) sender() {
	for task := range s.queue {
		if response, err := s.SendMessage(task.Connection, task.Message); err != nil {
			s.logger.Info("Error sending message:", err)
			s.logger.Info("Response: ", response)
		}
	}
}

type Responses struct {
	Result *transport.Message
	Error  error
}

func (s *TCPServer) SendToAll(message *transport.Message) []*Responses {
	s.mu.Lock()
	defer s.mu.Unlock()

	var wg sync.WaitGroup
	responseChan := make(chan *Responses, len(s.conns))

	for conn := range s.conns {
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			response, err := s.SendMessage(c, message)
			responseChan <- &Responses{
				Result: response,
				Error:  err,
			}
		}(conn)
	}

	wg.Wait()
	close(responseChan)

	responses := make([]*Responses, 0, len(s.conns))
	for response := range responseChan {
		responses = append(responses, response)
	}

	return responses
}

func (s *TCPServer) Stop() error {
	return s.listener.Close()
}

func (s *TCPServer) GetConnection() net.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.conns) == 0 {
		return nil
	}

	connections := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		connections = append(connections, conn)
	}

	h := rand.New(rand.NewSource(time.Now().UnixNano()))
	selected := connections[h.Intn(len(connections))]

	return selected
}
