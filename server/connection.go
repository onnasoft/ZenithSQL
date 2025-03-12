package server

import (
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/onnasoft/ZenithSQL/transport"
)

type Sender struct {
	Message      *transport.Message
	Silent       bool
	ResponseChan chan *transport.Message
}

type ConnectionHandler struct {
	conn         net.Conn
	sendQueue    chan *transport.Message
	messageQueue chan *transport.Message
	closeChan    chan struct{}
	server       *MessageServer
	isClosed     bool
	mu           sync.Mutex
	responseMap  map[string]chan *transport.Message
	NodeID       string
}

func NewConnectionHandler(conn net.Conn, server *MessageServer) *ConnectionHandler {
	handler := &ConnectionHandler{
		conn:         conn,
		sendQueue:    make(chan *transport.Message, 100),
		messageQueue: make(chan *transport.Message, 100),
		closeChan:    make(chan struct{}),
		server:       server,
	}

	go handler.startReader()
	go handler.startWriter()
	return handler
}

func (h *ConnectionHandler) startReader() {
	defer recoverFromPanic("startReader", h.server)
	defer h.Close()

	for {
		header, body, err := h.server.readMessage(h.conn)
		if err != nil {
			h.server.logger.Error(fmt.Sprintf("Error reading message from %s: %s", h.conn.RemoteAddr(), err))
			return
		}

		message, err := transport.ParseStatement(header, body)
		if err != nil {
			h.server.logger.Error(fmt.Sprintf("Error parsing message from %s: %s", h.conn.RemoteAddr(), err))
			return
		}

		if h.server.handlePingMessage(h, message) {
			continue
		}

		messageID := hex.EncodeToString(message.Header.MessageID[:])
		h.mu.Lock()
		if ch, ok := h.responseMap[messageID]; ok {
			ch <- message
			delete(h.responseMap, messageID)
		} else {
			h.messageQueue <- message
		}
	}
}

func (h *ConnectionHandler) ReadMessage() *transport.Message {
	select {
	case response := <-h.messageQueue:
		return response
	case <-h.closeChan:
		return nil
	}
}

func (h *ConnectionHandler) startWriter() {
	defer recoverFromPanic("startWriter", h.server)

	for {
		select {
		case msg := <-h.sendQueue:
			if msg == nil {
				continue
			}
			_, err := h.conn.Write(msg.Serialize())
			if err != nil {
				h.server.logger.Error("Failed to send message to", h.conn.RemoteAddr(), ":", err)
				h.Close()
				return
			}

		case <-h.closeChan:
			return
		}
	}
}

func (h *ConnectionHandler) SendSilent(message *transport.Message) error {
	h.sendQueue <- message

	return nil
}

func (h *ConnectionHandler) Send(message *transport.Message) (*transport.Message, error) {
	err := h.SendSilent(message)
	if err != nil {
		return nil, fmt.Errorf("failed to send message: %w", err)
	}

	messageID := hex.EncodeToString(message.Header.MessageID[:])
	h.mu.Lock()
	responseChan := make(chan *transport.Message, 1)
	h.responseMap[messageID] = responseChan
	h.mu.Unlock()

	select {
	case response := <-responseChan:
		return response, nil
	case <-time.After(5 * time.Second):
		return nil, net.ErrClosed
	}
}

func (h *ConnectionHandler) Close() {
	if h.isClosed {
		return
	}
	h.isClosed = true
	close(h.sendQueue)
	close(h.messageQueue)
	close(h.closeChan)
	h.conn.Close()
	h.server.nodeManager.RemoveNodeConnection(h.NodeID, h)
}

func (h *ConnectionHandler) RemoteAddr() net.Addr {
	return h.conn.RemoteAddr()
}
