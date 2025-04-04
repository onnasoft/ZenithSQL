package messageclient

import "github.com/onnasoft/ZenithSQL/net/network"

type ConnectionPool struct {
	conn      *network.ZenithConnection
	loanCount int
	index     int
}
