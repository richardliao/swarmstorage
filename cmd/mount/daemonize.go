package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"time"
)

var cmdDaemonize = &Command{
	UsageLine: "daemonize cmd options",
	Short:     "make swarmmount run in daemon",
	Long: `
Command daemonize run swarm service in daemon. For example:
    swarmmount daemonize swarmmount mount -swarmservers 192.168.0.101,192.168.0.102
	`,
}

func init() {
	cmdDaemonize.Run = runDaemonize
}

func runDaemonize(cmd *Command, args []string) {
	// build command
	command := exec.Command(os.Args[2], os.Args[3:]...)

	// prepare stderr
	stderr, err := command.StderrPipe()
	if err != nil {
		fmt.Printf("Failed start %s, error: %s\n", os.Args[2], err)
		os.Exit(1)
		return
	}

	// run command
	err = command.Start()
	if err != nil {
		fmt.Printf("Failed start %s, error: %s\n", os.Args[2], err)
		os.Exit(1)
		return
	}

	// wait stderr for 1 second
	waitChan := make(chan []byte)
	go func() {
		// read stderr
		errinfo, err := ioutil.ReadAll(stderr)
		if err != nil {
			waitChan <- []byte(err.Error())
		}

		// send to chan
		if len(errinfo) != 0 {
			waitChan <- errinfo
		} else {
			waitChan <- []byte("")
		}
	}()

	// wait chan
	for {
		select {
		case errinfo := <-waitChan:
			// something wrong
			if len(errinfo) != 0 {
				fmt.Printf("%s failed, error:\n%s\n", os.Args[2], errinfo)
				os.Exit(1)
				return
			}
		case <-time.After(1e9):
			// nothing get from stderr, it is ok
			fmt.Printf("%s started\n", os.Args[2])
			return
		}
	}
}
