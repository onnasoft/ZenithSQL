package nodes

import (
	"net"
	"sync"

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
	Connections map[net.Conn]struct{}
	Replicas    []net.Conn
	mu          sync.Mutex
	logger      *logrus.Logger
}

func NewNode(id string, role NodeRole, logger *logrus.Logger) *Node {
	return &Node{
		ID:          id,
		Role:        role,
		Connections: make(map[net.Conn]struct{}),
		Replicas:    []net.Conn{},
		logger:      logger,
		mu:          sync.Mutex{},
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

func (n *Node) AddReplica(replicaConn net.Conn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Replicas = append(n.Replicas, replicaConn)
}

func (n *Node) RemoveReplica(replicaConn net.Conn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, conn := range n.Replicas {
		if conn == replicaConn {
			n.Replicas = append(n.Replicas[:i], n.Replicas[i+1:]...)
		}
	}
}
