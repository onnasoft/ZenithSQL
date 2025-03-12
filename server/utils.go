package server

import "github.com/onnasoft/ZenithSQL/transport"

type MessageResponse struct {
	Result *transport.Message
	Error  error
}
