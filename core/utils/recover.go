package utils

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

func RecoverFromPanic(funcName string, logger *logrus.Logger) {
	if r := recover(); r != nil {
		logger.Fatal(fmt.Sprintf("[PANIC] Recovered in %s: %v", funcName, r))
	}
}
