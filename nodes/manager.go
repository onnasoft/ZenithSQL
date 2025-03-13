package nodes

import (
	"sync"

	"github.com/sirupsen/logrus"
)

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

func (m *NodeManager) GetNode(id string) *Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	node, exists := m.nodes[id]
	if !exists {
		return nil
	}

	return node
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

func (m *NodeManager) ClearAllNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id := range m.nodes {
		m.RemoveNode(id)
	}
}
