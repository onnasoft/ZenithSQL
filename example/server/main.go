package main

import (
	"log"
	"time"

	"github.com/onnasoft/ZenithSQL/io/statement"
	"github.com/onnasoft/ZenithSQL/io/transport"
	"github.com/onnasoft/ZenithSQL/net/messageserver"
	"github.com/onnasoft/ZenithSQL/net/network"
	"github.com/sirupsen/logrus"
)

const (
	NUM_CLIENTS                = 200
	MAX_CONNECTIONS_PER_CLIENT = 200
	MIN_CONNECTIONS_PER_CLIENT = 100
	MESSAGES_PER_CLIENT        = 10000
	TIMEOUT                    = 3 * time.Second
	SERVER_ADDR                = "127.0.0.1:8081"
	TOKEN                      = "my-secure-token"
)

func main() {
	logger := logrus.New()

	svr := messageserver.NewMessageServer(&messageserver.ServerConfig{
		Address: ":8081",
		Logger:  logger,
		Timeout: 3 * time.Second,
		OnRequest: func(conn *network.ZenithConnection, header *transport.MessageHeader, stmt statement.Statement) {
			msg, _ := transport.NewResponseMessage(header, statement.NewEmptyStatement(stmt.Protocol()))

			_, err := conn.Write(msg.ToBytes())
			if err != nil {
				logger.Info("Failed to send response:", err)
			}
		},
		OnConnection: func(conn *network.ZenithConnection, stmt *statement.JoinClusterStatement) {
			logger.Info("New connection from ", conn.RemoteAddr(), stmt.Tags)
		},
		JoinValidator: func(stmt *statement.JoinClusterStatement) bool {
			return stmt.ValidateHash(TOKEN)
		},
	})

	go func() {
		err := svr.Start()
		if err != nil {
			log.Fatal("Failed to start server:", err)
		}
	}()

	logger.Info("Server started at ", SERVER_ADDR)

	<-make(chan struct{})
}
