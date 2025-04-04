package dto

import "github.com/onnasoft/ZenithSQL/io/protocol"

type Dto interface {
	Protocol() protocol.MessageType
	ToBytes() ([]byte, error)
	FromBytes(data []byte) error
	String() string
}
