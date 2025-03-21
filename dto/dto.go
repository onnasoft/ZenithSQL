package dto

import "github.com/onnasoft/ZenithSQL/protocol"

type Dto interface {
	Protocol() protocol.MessageType
	ToBytes() ([]byte, error)
	FromBytes(data []byte) error
	String() string
}
