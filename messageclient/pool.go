package messageclient

import "github.com/onnasoft/ZenithSQL/network"

type ConnectionPool struct {
	conn      *network.ZenithConnection
	loanCount int
	index     int
}
