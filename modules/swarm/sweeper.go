package swarm

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

//------------------------------------------------------------------------------
// Sweeper
//------------------------------------------------------------------------------

/*
Periodically check all blocks health by walking local devices.
*/
type Sweeper struct {
	WorkerBase
	watchdog *Watchdog
}

// Implement interface Restartable
func (this *Sweeper) Name() string {
	return "Sweeper"
}

// Implement interface Restartable
func (this *Sweeper) Start(r Restartable, e interface{}) {
	defer func() {
		e := recover()
		this.OnPanic(this, this.watchdog, e)
	}()

	logger.Critical("Sweeper started.")

	for {
		time.Sleep(randInterval(SWEEP_INTERVAL))
		this.doSweep()
	}

	logger.Critical("Sweeper stopped.")
}

// Do sweep once
func (this *Sweeper) doSweep() (err error) {
	if LOCAL_DEVICES.LocalDeviceNum() == 0 {
		return
	}
	for deviceId, deviceState := range LOCAL_DEVICES.Data {
		err = this.checkDevice(deviceId, deviceState.Path)
		if err != nil {
			logger.Error("Error in sweep, error: %s", err.Error())
			return
		}
	}
	return
}

// Sweep device
func (this *Sweeper) checkDevice(deviceId string, devicePath string) (err error) {
	blocksPath := filepath.Join(devicePath, BLOCKS_DIR)
	filepath.Walk(blocksPath, func(path string, info os.FileInfo, e error) (err error) {
		// sleep 1ms
		defer func() {
			time.Sleep(1 * time.Millisecond)
		}()

		// throttle
		throttle("sweep", deviceId)

		if e != nil {
			return e
		}

		// skip dir
		if info.IsDir() {
			return
		}

		// check filename
		blockAnyDevice, err := parseBlockFileName(info.Name())
		if err != nil {
			err = fmt.Errorf("Invalid block file: %s", info.Name())
			return
		}
		blockInfo := NewBlockInfoFromAnyDevice(deviceId, blockAnyDevice)

		// try read block from local
		_, err = readBlockLocalDevice(blockInfo, false)
		if err != nil {
			// remove from META_CACHE
			META_CACHE.DelBlockhash(blockInfo)

			// try del block from local
			err = delBlockLocalDevice(blockInfo)
			if err != nil {
				return
			}
		}
		return
	})
	return
}
