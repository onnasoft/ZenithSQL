package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/onnasoft/ZenithSQL/client"
	"github.com/onnasoft/ZenithSQL/network"
	"github.com/onnasoft/ZenithSQL/protocol"
	"github.com/onnasoft/ZenithSQL/response"
	"github.com/onnasoft/ZenithSQL/server"
	"github.com/onnasoft/ZenithSQL/statement"
	"github.com/onnasoft/ZenithSQL/transport"
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

var (
	successfulRequests int
	failedRequests     int
	totalLatency       time.Duration
	mu                 sync.Mutex
	clientInstance     *client.MessageClient
	once               sync.Once
)

func main() {
	logger := logrus.New()

	svr := server.NewMessageServer(&server.ServerConfig{
		Address: ":8081",
		Logger:  logger,
		Timeout: 3 * time.Second,
		Handler: func(conn *network.ZenithConnection, message *transport.Message) {
			response, _ := response.DeserializeResponse(message.Header.MessageType, message.Body)
			msg, _ := transport.NewResponseMessage(message, statement.NewEmptyStatement(response.Protocol()))
			msg.Header.MessageID = message.Header.MessageID
			msg.Header.MessageType = message.Header.MessageType

			_, err := conn.Write(msg.ToBytes())
			if err != nil {
				logger.Info("Failed to send response:", err)
			}
		},
		OnConnection: func(conn *network.ZenithConnection, stmt *statement.LoginStatement) {
			logger.Info("New connection from ", conn.RemoteAddr(), stmt.Tags)
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
	once.Do(func() {
		clientInstance = client.NewMessageClient(&client.MessageConfig{
			ServerAddr: SERVER_ADDR,
			Token:      TOKEN,
			NodeID:     "global_master",
			Tags:       []string{"master"},
			MaxConn:    MAX_CONNECTIONS_PER_CLIENT,
			MinConn:    MIN_CONNECTIONS_PER_CLIENT,
			Timeout:    TIMEOUT,
			Logger:     logger,
		})
	})

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
	conn, _ := clientInstance.AllocateConnection()
	if conn == nil {
		logger.Warn("Failed to borrow connection")
		return
	}
	defer clientInstance.FreeConnection(conn)

	for i := 0; i < MESSAGES_PER_CLIENT; i++ {
		func(i int) {
			startTime := time.Now()

			defer clientInstance.FreeConnection(conn)

			msg, _ := transport.NewMessage(protocol.CreateDatabase, &statement.CreateDatabaseStatement{
				DatabaseName: fmt.Sprintf("test_db_%d_%d", clientID, i),
			})

			_, err := conn.Send(msg)
			elapsed := time.Since(startTime)

			mu.Lock()
			if err != nil {
				failedRequests++
			} else {
				successfulRequests++
				totalLatency += elapsed
			}
			mu.Unlock()
		}(i)
	}
}
