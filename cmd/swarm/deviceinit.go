package main

import (
	"fmt"
	"github.com/richardliao/swarm/modules/swarm"
	"os"
	"path"
)

var cmdDeviceinit = &Command{
	UsageLine: "deviceinit options",
	Short:     "init swarm device",
	Long: `
Command deviceinit create new swarm device environment specified in nodebase.
	`,
}

var (
	deviceinitNodebase  = cmdDeviceinit.Flag.String("nodebase", "/data/swarm", "Path to the existing swarm node environment, initiated using nodeinit command")
	deviceinitDevice = cmdDeviceinit.Flag.String("device", "device1", "Mount directory name under directory devices, the device must be mounted by advance")
)

func init() {
	cmdDeviceinit.Run = runDeviceinit
}

func runDeviceinit(cmd *Command, args []string) {
	// verify flags
	if len(*deviceinitNodebase) == 0 {
		fmt.Fprintf(os.Stderr, "Invalid params.\n")
		os.Exit(1)
	}

	if len(*deviceinitDevice) == 0 {
		fmt.Fprintf(os.Stderr, "Invalid params.\n")
		os.Exit(1)
	}

	// verify deviceinitNodebase
	devicePath := path.Join(*deviceinitNodebase, swarm.DEVICES_DIR, *deviceinitDevice)
	if !swarm.ExistPath(devicePath) {
		fmt.Fprintf(os.Stderr, "Error: device path %s not exists\n", devicePath)
		os.Exit(1)
	}
	uuidPath := path.Join(devicePath, swarm.UUID_FILE)
	if swarm.ExistPath(uuidPath) {
		fmt.Fprintf(os.Stderr, "Error: uuid file %s exists already\n", uuidPath)
		os.Exit(1)
	}

	// prepare dirs
	blocksPath := path.Join(devicePath, swarm.BLOCKS_DIR)
	err := os.MkdirAll(blocksPath, 0755)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed make dir %s, error: %s.\n", blocksPath, err)
		os.Exit(1)
	}

	// prepare uuid
	f, err := swarm.CreateFile(uuidPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed create file %s, error: %s.\n", uuidPath, err)
		os.Exit(1)
	}
	defer f.Close()
	f.Write([]byte(swarm.Uuid()))
}
