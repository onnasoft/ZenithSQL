package network

import (
	"net"

	"github.com/onnasoft/ZenithSQL/utils"
	"github.com/sirupsen/logrus"
)

type PostgresListener struct {
	net.Listener

	logger  *logrus.Logger
	address string
}

type PostgresListenerConfig struct {
	Address string
	Logger  *logrus.Logger
}

func NewPostgresListener(config *PostgresListenerConfig) *PostgresListener {
	return &PostgresListener{
		address: config.Address,
		logger:  config.Logger,
	}
}

func (l *PostgresListener) Stop() error {
	l.logger.Info("Stopping listener")
	return l.Listener.Close()
}

func (l *PostgresListener) Start() error {
	listener, err := net.Listen("tcp", l.address)
	if err != nil {
		l.logger.Error("Failed to start listener:", err)
		return err
	}

	l.Listener = listener
	l.logger.Info("Listening on", l.address)

	for {
		conn, err := l.Accept()
		if err != nil {
			l.logger.Error("Failed to accept connection", err)
			return err
		}

		go l.handleConnection(conn)
	}
}

func (l *PostgresListener) handleConnection(conn net.Conn) {
	defer utils.RecoverFromPanic("handleConnection", l.logger)

}
