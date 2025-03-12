package main

import (
	"fmt"
	"net"
	"time"

	"github.com/onnasoft/sql-parser/client"
	"github.com/onnasoft/sql-parser/protocol"
	"github.com/onnasoft/sql-parser/server"
	"github.com/onnasoft/sql-parser/statement"
	"github.com/onnasoft/sql-parser/transport"
	"github.com/sirupsen/logrus"
)

func main() {
	logger := logrus.New()

	server := server.NewMessageServer(&server.ServerConfig{
		Port:   8081,
		Logger: logger,
		Handler: func(conn net.Conn, message *transport.Message) {
			fmt.Println("Received message:", string(message.Body))
		},
		LoginValidator: func(token string) bool {
			return token == "my-secure-token"
		},
	})

	go func() {
		err := server.Start()
		if err != nil {
			fmt.Println("Failed to start server:", err)
		}
	}()

	time.Sleep(1 * time.Second)
	client := client.NewMessageClient("127.0.0.1:8081", "my-secure-token", logger)

	err := client.Connect()
	if err != nil {
		fmt.Println("Failed to connect:", err)
		return
	}
	msg, _ := transport.NewMessage(protocol.CreateDatabase, &statement.CreateDatabaseStatement{DatabaseName: "mydb"})
	client.SendMessage(msg)

	<-time.After(1 * time.Minute)
}
