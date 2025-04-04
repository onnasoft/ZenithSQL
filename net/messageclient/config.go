package messageclient

import (
	"time"

	"github.com/onnasoft/ZenithSQL/io/transport"
	"github.com/onnasoft/ZenithSQL/net/network"
	"github.com/sirupsen/logrus"
)

type MessageConfig struct {
	ServerAddr string
	Token      string
	NodeID     string
	Tags       []string
	Logger     *logrus.Logger
	MinConn    int
	MaxConn    int
	Timeout    time.Duration

	OnConnection func()
	OnMessage    func(*network.ZenithConnection, *transport.Message)
	OnShutdown   func()
}
