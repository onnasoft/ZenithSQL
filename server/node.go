package server

import (
	"sync"

	"github.com/onnasoft/ZenithSQL/transport"
	"github.com/sirupsen/logrus"
)

type NodeRole int

const (
	Master NodeRole = iota
	Slave
)

type Node struct {
	ID          string
	Role        NodeRole
	Connections map[*ConnectionHandler]struct{}
	Replicas    []*ConnectionHandler
	mu          sync.Mutex
	logger      *logrus.Logger
}

func NewNode(id string, role NodeRole, logger *logrus.Logger) *Node {
	return &Node{
		ID:          id,
		Role:        role,
		Connections: make(map[*ConnectionHandler]struct{}),
		Replicas:    []*ConnectionHandler{},
		logger:      logger,
		mu:          sync.Mutex{},
	}
}

func (n *Node) AddConnection(conn *ConnectionHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Connections[conn] = struct{}{}
}

func (n *Node) RemoveConnection(conn *ConnectionHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.Connections, conn)
}

func (n *Node) AddReplica(replicaConn *ConnectionHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Replicas = append(n.Replicas, replicaConn)
}

func (n *Node) RemoveReplica(replicaConn *ConnectionHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, conn := range n.Replicas {
		if conn == replicaConn {
			n.Replicas = append(n.Replicas[:i], n.Replicas[i+1:]...)
			break
		}
	}
}

func (n *Node) SendMessage(message *transport.Message) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	var err error
	for conn := range n.Connections {
		if _, err = conn.Send(message); err != nil {
			n.logger.Error("Error sending message to", conn.RemoteAddr(), ":", err)
		}
	}

	for _, replicaConn := range n.Replicas {
		if _, err = replicaConn.Send(message); err != nil {
			n.logger.Error("Error sending message to replica", replicaConn.RemoteAddr(), ":", err)
		}
	}

	return err
}

type NodeManager struct {
	nodes   map[string]*Node
	masters map[string]*Node
	slaves  map[string]*Node
	mu      sync.RWMutex
	logger  *logrus.Logger
}

func NewNodeManager(logger *logrus.Logger) *NodeManager {
	return &NodeManager{
		nodes:   make(map[string]*Node),
		masters: make(map[string]*Node),
		slaves:  make(map[string]*Node),
		logger:  logger,
	}
}

func (m *NodeManager) AddNode(id string, role NodeRole) *Node {
	m.mu.Lock()
	defer m.mu.Unlock()

	if node, exists := m.nodes[id]; exists {
		return node
	}

	node := NewNode(id, role, m.logger)
	m.nodes[id] = node

	if role == Master {
		m.masters[id] = node
	} else {
		m.slaves[id] = node
	}

	return node
}

func (m *NodeManager) RemoveNode(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[id]
	if !exists {
		return
	}

	for conn := range node.Connections {
		conn.Close()
	}

	delete(m.nodes, id)

	if node.Role == Master {
		delete(m.masters, id)
	} else {
		delete(m.slaves, id)
	}
}

func (m *NodeManager) GetNode(id string) *Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	node := m.nodes[id]
	return node
}

func (m *NodeManager) GetMasters() map[string]*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.masters
}

func (m *NodeManager) GetSlaves() map[string]*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.slaves
}

func (m *NodeManager) CountNodes() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.nodes)
}

func (m *NodeManager) GetAllNodes() map[string]*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nodes
}

func (m *NodeManager) ClearAllNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, node := range m.nodes {
		for conn := range node.Connections {
			conn.Close()
		}
		delete(m.nodes, id)
	}

	m.masters = make(map[string]*Node)
	m.slaves = make(map[string]*Node)
}

func (m *NodeManager) GetNodeByConnection(conn *ConnectionHandler) *Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, node := range m.nodes {
		if _, exists := node.Connections[conn]; exists {
			return node
		}
	}

	return nil
}

func (m *NodeManager) RemoveNodeConnection(nodeId string, conn *ConnectionHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[nodeId]
	if !exists {
		return
	}

	node.RemoveConnection(conn)
}
