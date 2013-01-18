package main

import (
	"fmt"
	"github.com/richardliao/swarm/modules/swarm"
)

var cmdVersion = &Command{
	UsageLine: "version",
	Short:     "show version",
	Long: `
Show swarmmount version.
	`,
}

func init() {
	cmdVersion.Run = runVersion
}

func runVersion(cmd *Command, args []string) {
	fmt.Printf("swarmmount version %s\n", swarm.SWARM_VERSION)
}
