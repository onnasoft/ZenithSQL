package server

import (
	"crypto/tls"
	"log"
	"net"
	"time"

	"github.com/onnasoft/ZenithSQL/statement"
	"github.com/onnasoft/ZenithSQL/transport"
	"github.com/sirupsen/logrus"
)

type ServerConfig struct {
	Address        string
	Timeout        time.Duration
	Logger         *logrus.Logger
	Handler        func(net.Conn, *transport.Message)
	LoginValidator func(*statement.LoginStatement) bool
	CertFile       string
	KeyFile        string

	OnListening func()
	OnShutdown  func()
}

func loadTLSConfig(cfg *ServerConfig) *tls.Config {
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			log.Fatal("Failed to load TLS certificate:", err)
		}
		cfg.Logger.Info("TLS enabled")
		return &tls.Config{Certificates: []tls.Certificate{cert}}
	}
	return nil
}
