package server

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/onnasoft/sql-parser/statement"
	"github.com/onnasoft/sql-parser/transport"
	"github.com/sirupsen/logrus"
)

type MessageTask struct {
	Message    *transport.Message
	Connection net.Conn
}

type Node struct {
	ID          string
	Connections map[net.Conn]struct{}
	Tags        map[string]struct{}
	mu          sync.Mutex
}

func NewNode(id string, tags []string) *Node {
	tagSet := make(map[string]struct{})
	for _, tag := range tags {
		tagSet[tag] = struct{}{}
	}

	return &Node{
		ID:          id,
		Connections: make(map[net.Conn]struct{}),
		Tags:        tagSet,
	}
}

func (n *Node) AddConnection(conn net.Conn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Connections[conn] = struct{}{}
}

func (n *Node) RemoveConnection(conn net.Conn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.Connections, conn)
}

func (n *Node) HasTag(tag string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, exists := n.Tags[tag]
	return exists
}

type MessageServer struct {
	listener       net.Listener
	nodes          map[string]*Node
	taskQueue      chan *MessageTask
	responseMap    map[string]chan *transport.Message
	port           int
	logger         *logrus.Logger
	messageHandler func(net.Conn, *transport.Message)
	loginValidator func(*statement.LoginStatement) bool
	mu             sync.Mutex
}

type ServerConfig struct {
	Port           int
	Logger         *logrus.Logger
	Handler        func(net.Conn, *transport.Message)
	LoginValidator func(*statement.LoginStatement) bool
}

func NewMessageServer(cfg *ServerConfig) *MessageServer {
	return &MessageServer{
		port:           cfg.Port,
		logger:         cfg.Logger,
		messageHandler: cfg.Handler,
		loginValidator: cfg.LoginValidator,
		nodes:          make(map[string]*Node),
		taskQueue:      make(chan *MessageTask),
		responseMap:    make(map[string]chan *transport.Message),
	}
}

func (s *MessageServer) Start() error {
	defer func() {
		s.mu.Lock()
		for _, node := range s.nodes {
			for conn := range node.Connections {
				conn.Close()
			}
		}
		s.nodes = make(map[string]*Node)
		close(s.taskQueue)
		s.mu.Unlock()
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
	responseChan := make(chan *MessageResponse, len(s.nodes))

	wg.Add(len(s.nodes))
	for _, node := range s.nodes {
		for conn := range node.Connections {
			go func(c net.Conn) {
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

	responses := make([]*MessageResponse, 0, len(s.nodes))
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

func (s *MessageServer) RegisterNode(id string, tags []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.nodes[id]; !exists {
		s.nodes[id] = NewNode(id, tags)
	}
}

func (s *MessageServer) GetNodesByTag(tag string) []*Node {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []*Node
	for _, node := range s.nodes {
		if node.HasTag(tag) {
			result = append(result, node)
		}
	}
	return result
}

func (s *MessageServer) GetRandomNode() *Node {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.nodes) == 0 {
		return nil
	}

	nodeList := make([]*Node, 0, len(s.nodes))
	for _, node := range s.nodes {
		nodeList = append(nodeList, node)
	}

	h := rand.New(rand.NewSource(time.Now().UnixNano()))
	return nodeList[h.Intn(len(nodeList))]
}

func (s *MessageServer) SendSilentMessage(conn net.Conn, message *transport.Message) error {
	_, err := conn.Write(message.Serialize())
	return err
}

func (s *MessageServer) SendMessage(conn net.Conn, message *transport.Message) (*transport.Message, error) {
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
		return response, nil
	case <-time.After(5 * time.Second):
		return nil, net.ErrClosed
	}
}

func (s *MessageServer) closeConnection(conn net.Conn, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, node := range s.nodes {
		if _, exists := node.Connections[conn]; exists {
			delete(node.Connections, conn)
			conn.Close()
			s.logger.Info("Connection closed:", conn.RemoteAddr(), " Reason:", reason)
			return
		}
	}
}
