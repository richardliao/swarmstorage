package main

import (
	"fmt"
	"github.com/richardliao/swarm/modules/swarm"
	"os"
)

var cmdServe = &Command{
	UsageLine: "serve options",
	Short:     "serve swarm server",
	Long: `
Command serve starts swarm server with given nodebase path.
	`,
}

var (
	serveNodebase = cmdServe.Flag.String("nodebase", "/data/swarm", "Path to the existing swarm node environment, initiated using nodeinit command")
)

func init() {
	cmdServe.Run = runServe
}

func runServe(cmd *Command, args []string) {
	// verify flags
	if len(*serveNodebase) == 0 {
		fmt.Fprintf(os.Stderr, "Invalid params.\n")
		os.Exit(1)
	}

	// verify serveNodebase
	if !swarm.ExistPath(*serveNodebase) {
		fmt.Fprintf(os.Stderr, "Path %s not exists.\n", *serveNodebase)
		os.Exit(1)
	}

	// set NODE_BASE_PATH
	swarm.NODE_BASE_PATH = *serveNodebase

	swarm.Start()

}
