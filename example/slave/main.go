package main

import (
	"log"
	"time"

	"github.com/onnasoft/ZenithSQL/messageclient"
	"github.com/onnasoft/ZenithSQL/messageserver"
	"github.com/onnasoft/ZenithSQL/network"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/onnasoft/ZenithSQL/statement"
	"github.com/onnasoft/ZenithSQL/transport"
	"github.com/sirupsen/logrus"
)

const (
	MAX_CONNECTIONS_PER_CLIENT = 1
	MIN_CONNECTIONS_PER_CLIENT = 1
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
		OnMessage: func(conn *network.ZenithConnection, message *transport.Message) {
			logger.Infof("Received message on server from %s: %s", conn.RemoteAddr(), message.Header.MessageType)
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

	time.Sleep(1 * time.Second)

	messageclient := messageclient.NewMessageClient(&messageclient.MessageConfig{
		ServerAddr: SERVER_ADDR,
		Token:      TOKEN,
		NodeID:     "slave_0",
		Tags:       []string{"slave"},
		MaxConn:    MAX_CONNECTIONS_PER_CLIENT,
		MinConn:    MIN_CONNECTIONS_PER_CLIENT,
		Timeout:    TIMEOUT,
		Logger:     logger,
		OnMessage: func(conn *network.ZenithConnection, message *transport.Message) {
			logger.Infof("Received message on client from %s: %s", conn.RemoteAddr(), message.Header.MessageType)

			stmt := statement.NewEmptyStatement(protocol.Welcome)
			response, _ := transport.NewResponseMessage(message, stmt)
			_, err := conn.Write(response.ToBytes())
			if err != nil {
				logger.Info("Failed to send response:", err)
			}
		},
	})

	logger.Info("Client started at ", messageclient.ServerAddr())
	logger.Info("Server started at ", svr.Addr())

	for i := 0; i < 10; i++ {
		logger.Info("Sending message: ", i)
		stmt, err := statement.NewMasterConnectedStatement("master")
		if err != nil {
			logger.Info("Failed to create statement: ", err)
			return
		}

		msg, err := transport.NewMessage(protocol.MasterConnected, stmt)
		if err != nil {
			logger.Info("Failed to create message: ", err)
			return
		}

		result := svr.SendToAllSlaves(msg)
		logger.Info("SendToAllSlaves result: ", len(result))
	}

	<-make(chan struct{})
}
