package main

import (
	"fmt"
	"log"
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

	var svr *server.MessageServer

	svr = server.NewMessageServer(&server.ServerConfig{
		Port:   8081,
		Logger: logger,
		Handler: func(conn net.Conn, message *transport.Message) {
			fmt.Println("Received message:", message.Stmt)
			msg, _ := transport.NewMessage(message.Stmt.Protocol(), statement.NewEmptyStatement(message.Stmt.Protocol()))
			msg.Header.MessageType = message.Header.MessageType

			err := svr.SendSilentMessage(conn, msg)
			if err != nil {
				fmt.Println("Failed to send response:", err)
			}
		},
		LoginValidator: func(stmt *statement.LoginStatement) bool {
			return stmt.ValidateHash("my-secure-token")
		},
	})

	go func() {
		err := svr.Start()
		if err != nil {
			fmt.Println("Failed to start server:", err)
		}
	}()

	time.Sleep(1 * time.Second)

	// Iniciamos el cliente con soporte para múltiples conexiones
	client := client.NewMessageClient("127.0.0.1:8081", "my-secure-token", logger, 1, 3*time.Second)

	client.Connect()

	// Enviamos mensajes en paralelo utilizando las conexiones activas
	/*for i := 0; i < 5; i++ {
		go func(index int) {
			msg, _ := transport.NewMessage(protocol.CreateDatabase, &statement.CreateDatabaseStatement{
				DatabaseName: fmt.Sprintf("mydb_%d", index),
			})
			resp, err := client.SendMessage(msg)
			if err != nil {
				fmt.Println("Failed to send message:", err)
			} else {
				fmt.Println("Response received:", resp)
			}
		}(i)
	}*/

	msg, _ := transport.NewMessage(protocol.CreateDatabase, &statement.CreateDatabaseStatement{
		DatabaseName: "mydb_index",
	})
	resp, err := client.SendMessage(msg)
	if err != nil {
		log.Fatal("[here] Failed to send message: ", err)
	} else {
		fmt.Println("Response received:", resp)
	}

	// Mantener la ejecución por 1 minuto para pruebas
	<-time.After(1 * time.Minute)
}
