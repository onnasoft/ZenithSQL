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
	Tags        map[string]struct{}
	Address     string
}

type NodeConfig struct {
	ID      string
	Role    NodeRole
	Tags    map[string]struct{}
	Address string
	Logger  *logrus.Logger
}

func NewNode(config *NodeConfig) *Node {
	return &Node{
		ID:          config.ID,
		Role:        config.Role,
		Connections: make(map[net.Conn]struct{}),
		Replicas:    make([]net.Conn, 0),
		logger:      config.Logger,
		Tags:        config.Tags,
		Address:     config.Address,
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
