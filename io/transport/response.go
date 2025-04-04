package transport

import "errors"

type ExecutionResult struct {
	Result interface{}
	Error  error
}

var ErrTimeout = errors.New("timeout error")
