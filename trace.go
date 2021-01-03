package dynamic

import (
	"fmt"
	"log"
	"os"
)

// InfoFunc is a tracing function
type InfoFunc func(format string, args ...interface{})

var (
	logger    = log.New(os.Stdout, "INFO: ", log.Lshortfile)
	errLogger = log.New(os.Stdout, "ERROR: ", log.Lshortfile)

	// Infof is a basic logging function
	Infof = func(format string, args ...interface{}) {
		_ = logger.Output(2, fmt.Sprintf(format, args...))
	}

	// Errorf is a basic error logging function
	Errorf = func(err error) {
		_ = errLogger.Output(2, err.Error())
	}
)
