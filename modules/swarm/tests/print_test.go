package tests

import (
	"fmt"
	"github.com/richardliao/swarm/third_party/code.google.com/p/log4go"
	"testing"
)

func TestPrintf(t *testing.T) {
	fmt.Printf("Test printf.\n")
}

const (
	LogFileName = "/data/logs/swarm/swarm.log"
)

func TestLog4go(t *testing.T) {
	logger := log4go.NewLogger()
	logger.AddFilter("file", log4go.FINE, log4go.NewFileLogWriter(LogFileName, false))

	logger.Info("Everything is created now.")
	logger.Critical("Time to close out!")

	logger.Close()
}
