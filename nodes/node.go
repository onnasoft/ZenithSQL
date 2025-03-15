package nodes

import (
	"net"
	"sync"

	"github.com/onnasoft/ZenithSQL/network"
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
	Connections map[*network.ZenithConnection]struct{}
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
		Connections: make(map[*network.ZenithConnection]struct{}),
		Replicas:    make([]net.Conn, 0),
		logger:      config.Logger,
		Tags:        config.Tags,
		Address:     config.Address,
	}
}

func (n *Node) AddConnection(conn *network.ZenithConnection) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Connections[conn] = struct{}{}
}

func (n *Node) RemoveConnection(conn *network.ZenithConnection) {
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

func (n *Node) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for conn := range n.Connections {
		conn.Close()
	}
	for _, replica := range n.Replicas {
		replica.Close()
	}
}

func (n *Node) Send(message *transport.Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for conn := range n.Connections {
		conn.Send(message)
		break
	}
}
