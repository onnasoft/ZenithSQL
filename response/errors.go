package response

import "errors"

var ErrUnsupportedResponse = errors.New("unsupported response type")

func NewErrUnsupportedResponse() error {
	return ErrUnsupportedResponse
}
