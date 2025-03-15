package nodes

import (
	"sync"
	"time"

	"github.com/onnasoft/ZenithSQL/statement"
	"github.com/onnasoft/ZenithSQL/transport"
	"github.com/sirupsen/logrus"
)

type NodeManager struct {
	nodes       map[string]*Node
	masters     map[string]*Node
	slaves      map[string]*Node
	taggedNodes map[string]map[string]*Node
	mu          sync.RWMutex
	logger      *logrus.Logger
	timeout     time.Duration
}

func NewNodeManager(logger *logrus.Logger) *NodeManager {
	return &NodeManager{
		nodes:       make(map[string]*Node),
		masters:     make(map[string]*Node),
		slaves:      make(map[string]*Node),
		taggedNodes: make(map[string]map[string]*Node),
		logger:      logger,
		timeout:     3 * time.Second,
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

func (m *NodeManager) AddNode(stmt *statement.LoginStatement, role NodeRole) *Node {
	m.mu.Lock()
	defer m.mu.Unlock()

	if node, exists := m.nodes[stmt.NodeID]; exists {
		return node
	}

	tags := make(map[string]struct{})
	for _, tag := range stmt.Tags {
		tags[tag] = struct{}{}
	}

	node := NewNode(&NodeConfig{
		ID:      stmt.NodeID,
		Role:    role,
		Tags:    tags,
		Address: stmt.Address,
		Logger:  m.logger,
	})
	node.Address = stmt.Address
	m.nodes[stmt.NodeID] = node

	for _, tag := range stmt.Tags {
		if _, exists := m.taggedNodes[tag]; !exists {
			m.taggedNodes[tag] = make(map[string]*Node)
		}
		m.taggedNodes[tag][stmt.NodeID] = node
	}

	if role == Master {
		m.masters[stmt.NodeID] = node
	} else {
		m.slaves[stmt.NodeID] = node
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

func (m *NodeManager) GetRandomNode() *Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, node := range m.nodes {
		return node
	}

	return nil
}

func (m *NodeManager) SendToAllSlaves(msg *transport.Message) []*transport.ExecutionResult {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var wg sync.WaitGroup
	responses := make(chan *transport.ExecutionResult, len(m.nodes))
	results := make([]*transport.ExecutionResult, 0, len(m.nodes))

	wg.Add(len(m.nodes))

	for _, node := range m.slaves {
		go func(node *Node) {
			defer wg.Done()
			respChan := make(chan *transport.ExecutionResult, 1)
			go func() {
				response, err := node.Send(msg)
				respChan <- &transport.ExecutionResult{
					Result: response,
					Error:  err,
				}
			}()

			select {
			case res := <-respChan:
				responses <- res
			case <-time.After(m.timeout):
				responses <- &transport.ExecutionResult{
					Result: nil,
					Error:  transport.ErrTimeout,
				}
			}
		}(node)
	}

	go func() {
		wg.Wait()
		close(responses)
	}()

	for response := range responses {
		results = append(results, response)
	}

	return results
}
