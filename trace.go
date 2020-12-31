package main

import (
	"fmt"
	"log"
	"os"
)

// InfoFunc is a tracing function
type InfoFunc func(format string, args ...interface{})

var (
	logger = log.New(os.Stdout, "INFO: ", log.Lshortfile)

	// Infof is a basic logging function for debug purposes
	Infof = func(format string, args ...interface{}) {
		logger.Output(2, fmt.Sprintf(format, args...))
	}
)
