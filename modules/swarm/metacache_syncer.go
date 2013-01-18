package swarm

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"
)

// Global

var META_CACHE MetaCache

func init() {
	META_CACHE = NewMetaCache()
}

//------------------------------------------------------------------------------
// MetaCacheSyncer
//------------------------------------------------------------------------------

/*
Periodically walk local devices to get block meta and save to META_CACHE, 
and tranverse META_CACHE to find and cleanup missing ones.
*/
type MetaCacheSyncer struct {
	WorkerBase
	watchdog *Watchdog
}

// Implement interface Restartable
func (this *MetaCacheSyncer) Name() string {
	return "MetaCacheSyncer"
}

// Implement interface Restartable
func (this *MetaCacheSyncer) Start(r Restartable, e interface{}) {
	defer func() {
		e := recover()
		this.OnPanic(this, this.watchdog, e)
	}()

	logger.Critical("MetaCacheSyncer started.")

	for {
		this.gatherMetaCache()
		if !META_CACHE.Ready {
			META_CACHE.Ready = true
		}
		this.cleanupMetaCache()
		time.Sleep(randInterval(BLOCK_META_SYNC_INTERVAL))
	}

	logger.Critical("MetaCacheSyncer stopped.")
}

// Gather block meta, include offline and removed
func (this *MetaCacheSyncer) gatherMetaCache() {
	if LOCAL_DEVICES.LocalDeviceNum() == 0 {
		logger.Debug("Can not gather meta cache when local devices are not ready.")
		return
	}
	for deviceId, deviceState := range LOCAL_DEVICES.Data {
		err := this.updateDeviceMeta(deviceId, deviceState.Path)
		if err != nil {
			logger.Error("Error in gatherMetaCache, error: %s", err.Error())
			return
		}
	}
	return
}

// Update device meta
func (this *MetaCacheSyncer) updateDeviceMeta(deviceId string, devicePath string) (err error) {
	blocksPath := filepath.Join(devicePath, BLOCKS_DIR)
	filepath.Walk(blocksPath, func(path string, info os.FileInfo, e error) (err error) {
		// sleep 1ms
		defer func() {
			// full speed when bootup
			if META_CACHE.Ready {
				time.Sleep(1e6)
			}
		}()

		// throttle
		throttle("updateDeviceMeta", deviceId)

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
		META_CACHE.Save(blockInfo)
		return
	})
	return
}

// Cleanup meta cache
func (this *MetaCacheSyncer) cleanupMetaCache() {
	for deviceId, typeBlocks := range META_CACHE.Data {
		for blocktype, blockList := range typeBlocks.Data {
			for i := blockList.Range(0, math.MaxUint32); i.Next(); {
				for _, blockhash := range i.Value().(*StringSet).Sorted() {
					// throttle
					throttle("cleanupMetaCache", deviceId)

					blockInfo := NewBlockInfo(deviceId, blocktype, blockhash)
					blockPath, err := getBlockPath(blockInfo)
					if err != nil {
						continue
					}

					if !ExistPath(blockPath) {
						// cleanup
						// TODO: recover
						META_CACHE.DelBlockhash(blockInfo)
						logger.Info("Cleanup block meta %s in path %s", blockInfo, blockPath)

						MONITOR.SetVariable("M:MC:CleanupMetaCache", 1)
					}

					// sleep 0.1ms
					time.Sleep(1e5)
				}
			}
		}
	}
	return
}
