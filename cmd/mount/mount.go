package main

import (
	"fmt"
	"github.com/richardliao/swarm/modules/fs"
	"github.com/richardliao/swarm/modules/swarm"
	"github.com/richardliao/swarm/third_party/github.com/hanwen/go-fuse/fuse"
	"os"
	"time"
)

var cmdMount = &Command{
	UsageLine: "mount options",
	Short:     "mount swarm in specified path",
	Long: `
Mount swarm in specified path.
	`,
}

var (
	mountMountpoint   = cmdMount.Flag.String("mountpoint", "/mnt/swarm", "Fuse mountpoint path, must exist")
	mountSwarmservers = cmdMount.Flag.String("swarmservers", "", "Swarm OSS addresses, seperated by comma. For example: 192.168.0.101,192.168.0.102")
	mountLogfile      = cmdMount.Flag.String("logfile", "/var/log/swarmfs.log", "Path to log file")
	mountDebug        = cmdMount.Flag.Bool("debug", false, "Print debugging messages")
)

func init() {
	cmdMount.Run = runMount
}

func runMount(cmd *Command, args []string) {
	// verify flags
	if len(*mountMountpoint) == 0 || len(*mountSwarmservers) == 0 {
		fmt.Fprintf(os.Stderr, "Invalid params.\n")
		os.Exit(1)
	}

	// verify mountMountpoint
	if !swarm.ExistPath(*mountMountpoint) {
		fmt.Fprintf(os.Stderr, "Path %s not exists.\n", *mountMountpoint)
		os.Exit(1)
	}

	var finalFs fuse.FileSystem
	swarmfs := fs.NewSwarmFS(*mountSwarmservers, *mountLogfile)
	finalFs = swarmfs

	opts := &fuse.FileSystemOptions{
		// These options are to be compatible with libfuse defaults,
		// making benchmarking easier.
		NegativeTimeout: time.Second,
		AttrTimeout:     time.Second,
		EntryTimeout:    time.Second,
	}
	pathFs := fuse.NewPathNodeFs(finalFs, nil)
	conn := fuse.NewFileSystemConnector(pathFs, opts)
	state := fuse.NewMountState(conn)
	state.Debug = *mountDebug

	fmt.Println("Mounting")
	err := state.Mount(*mountMountpoint, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Mount fail: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Mounted!")
	state.Loop()
}
