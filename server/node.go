package server

import (
	"net"
	"sync"
)

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
