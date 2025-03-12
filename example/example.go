package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/onnasoft/sql-parser/client"
	"github.com/onnasoft/sql-parser/protocol"
	"github.com/onnasoft/sql-parser/server"
	"github.com/onnasoft/sql-parser/statement"
	"github.com/onnasoft/sql-parser/transport"
	"github.com/sirupsen/logrus"
)

const (
	NUM_CLIENTS         = 100
	MESSAGES_PER_CLIENT = 20000
	TIMEOUT             = 3 * time.Second
	SERVER_ADDR         = "127.0.0.1:8081"
	TOKEN               = "my-secure-token"
)

var (
	successfulRequests int
	failedRequests     int
	totalLatency       time.Duration
	mu                 sync.Mutex
)

func main() {
	logger := logrus.New()

	var svr *server.MessageServer

	svr = server.NewMessageServer(&server.ServerConfig{
		Port:   8081,
		Logger: logger,
		Handler: func(conn net.Conn, message *transport.Message) {
			msg, _ := transport.NewMessage(message.Stmt.Protocol(), statement.NewEmptyStatement(message.Stmt.Protocol()))
			msg.Header.MessageID = message.Header.MessageID
			msg.Header.MessageType = message.Header.MessageType

			err := svr.SendSilentMessage(conn, msg)
			if err != nil {
				logger.Info("Failed to send response:", err)
			}
		},
		LoginValidator: func(stmt *statement.LoginStatement) bool {
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
	runLoadTest(logger)
}

func runLoadTest(logger *logrus.Logger) {
	var wg sync.WaitGroup
	wg.Add(NUM_CLIENTS)

	startTestTime := time.Now()

	for i := 0; i < NUM_CLIENTS; i++ {
		go func(clientID int) {
			defer wg.Done()
			runClient(clientID, logger)
		}(i)
	}

	wg.Wait()

	totalTime := time.Since(startTestTime)
	requestsPerSecond := float64(successfulRequests) / totalTime.Seconds()

	fmt.Println("=== Load Test Results ===")
	fmt.Printf("Total Clients: %d\n", NUM_CLIENTS)
	fmt.Printf("Messages Per Client: %d\n", MESSAGES_PER_CLIENT)
	fmt.Printf("Total Requests Sent: %d\n", NUM_CLIENTS*MESSAGES_PER_CLIENT)
	fmt.Printf("Successful Requests: %d\n", successfulRequests)
	fmt.Printf("Failed Requests: %d\n", failedRequests)
	fmt.Printf("Total Test Time: %v\n", totalTime)
	fmt.Printf("Requests Per Second (RPS): %.2f\n", requestsPerSecond)
	if successfulRequests > 0 {
		fmt.Printf("Average Latency: %v\n", totalLatency/time.Duration(successfulRequests))
	}
}

func runClient(clientID int, logger *logrus.Logger) {
	client := client.NewMessageClient(&client.MessageConfig{
		ServerAddr: SERVER_ADDR,
		Token:      TOKEN,
		NodeID:     fmt.Sprintf("client_%d", clientID),
		Tags:       []string{"client"},
		MaxConn:    10,
		Timeout:    TIMEOUT,
		Logger:     logger,
	})
	client.Connect()

	for i := 0; i < MESSAGES_PER_CLIENT; i++ {
		startTime := time.Now()

		msg, _ := transport.NewMessage(protocol.CreateDatabase, &statement.CreateDatabaseStatement{
			DatabaseName: fmt.Sprintf("test_db_%d_%d", clientID, i),
		})

		_, err := client.SendMessage(msg)
		elapsed := time.Since(startTime)

		mu.Lock()
		if err != nil {
			failedRequests++
		} else {
			successfulRequests++
			totalLatency += elapsed
		}
		mu.Unlock()
	}
}
