package server

import "fmt"

func recoverFromPanic(funcName string, s *MessageServer) {
	if r := recover(); r != nil {
		s.logger.Fatal(fmt.Sprintf("[PANIC] Recovered in %s: %v", funcName, r))
	}
}
