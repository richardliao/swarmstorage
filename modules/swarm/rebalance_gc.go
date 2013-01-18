package swarm

import (
	"fmt"
	"os"
	"time"
)

//------------------------------------------------------------------------------
// RebalanceGC
//------------------------------------------------------------------------------

/*
Periodically pick a rebalance gc seg, check blocks one by one. If the block is 
replicated, gc it from local device.

Most of the time, gc a specific block is one shot procedure, which will not occur 
again. It is unnecessary to keep a gc cache to speed up the process. 

It is not feasible to sync gc seg blocks to remote replicas, since mostly the 
local seg blocks to be gced are much less than remote, sync seg always means to 
push whole seg blocks. So, if any blocks are not replicated, push whole seg 
blocks to remote primary node, and wait them be synced to other replicas.

TODO: batch check blocks to speed up the process.
*/
type RebalanceGC struct {
	WorkerBase
	watchdog  *Watchdog
	semaphore *Semaphore
}

// Implement interface Restartable
func (this *RebalanceGC) Name() string {
	return "RebalanceGC"
}

// Implement interface Restartable
func (this *RebalanceGC) Start(r Restartable, e interface{}) {
	defer func() {
		e := recover()
		this.OnPanic(this, this.watchdog, e)
	}()

	logger.Critical("RebalanceGC started.")

	for {
		time.Sleep(randInterval(GC_INTERVAL))

		this.semaphore.Acquire()
		go this.doRebalanceGC()
	}

	logger.Critical("RebalanceGC stopped.")
}

// Do rebalance GC
func (this *RebalanceGC) doRebalanceGC() {
	defer func() {
		time.Sleep(randInterval(GC_INTERVAL))
		this.semaphore.Release()
	}()

	if !META_CACHE.Ready {
		// not ready
		return
	}

	// pick seg
	deviceIdLocal, scoreSeg, blocktype, err := randomGCSeg()
	if err != nil {
		return
	}

	// not online
	if !LOCAL_DEVICES.IsLocalOnlineDevice(deviceIdLocal) {
		return
	}

	// get primaryTick
	primaryTick, err := DOMAIN_RING.GetPrimaryTickByScore(scoreSeg.From)
	if err != nil {
		return
	}
	//if primaryTick.Name == deviceIdLocal {
	//    // same device
	//    return
	//}

	primaryNodeIp, ok := DEVICE_MAPPING[primaryTick.Name]
	if !ok {
		logger.Debug("Invalid DEVICE_MAPPING to get device %s", primaryTick.Name)
		return
	}

	// try gc
	emptied, err := tryGCSegBlocks(scoreSeg, deviceIdLocal, blocktype)
	if err != nil {
		return
	}

	if !emptied {
		nodeIpRemote, deviceIdRemote := primaryNodeIp, primaryTick.Name

		// some check
		nodeState, ok := DOMAIN_MAP.Get(nodeIpRemote)
		if !ok {
			return
		}
		if nodeState.Status() != NODE_STATUS_UP {
			// skip down node
			return
		}
		if err := canPushRebalance(nodeIpRemote); err != nil {
			return
		}

		// push whole seg blocks to remote replica
		syncBlocks, err := getSegAllBlocks(deviceIdLocal, blocktype, scoreSeg)
		if err != nil {
			return
		}

		// push
		err = requestRebalancePush(deviceIdLocal, nodeIpRemote, deviceIdRemote, blocktype, syncBlocks)
		if err != nil {
			return
		}
	}
}

// Pick a random GC seg
func randomGCSeg() (deviceIdLocal string, scoreSeg ScoreSeg, blocktype string, err error) {
	deviceIdLocal, err = randomDevice()
	if err != nil {
		return
	}

	// get seg
	gcSegs, err := REBALANCE_CACHE.GetGcSegs(deviceIdLocal)
	if err != nil {
		return
	}

	keys := make([]ScoreSeg, 0)
	for scoreSeg := range gcSegs.Data {
		keys = append(keys, scoreSeg)
	}

	scoreSeg, err = ChoiceScoreSeg(keys)
	if err != nil {
		return
	}

	blocktype, _ = ChoiceString(AVAILABLE_BLOCK_TYPES)
	return
}

// Get all blocks of a seg
func getSegAllBlocks(deviceIdLocal string, blocktype string, scoreSeg ScoreSeg) (syncBlocks *StringSet, err error) {
	syncBlocks = NewStringSet()

	// get blockList
	typeBlocks, ok := META_CACHE.Get(deviceIdLocal)
	if !ok {
		// empty
		return
	}
	blockList, ok := typeBlocks.Get(blocktype)
	if !ok {
		// empty
		return
	}

	for i := blockList.Range(scoreSeg.From, scoreSeg.To); i.Next(); {
		for _, blockhash := range i.Value().(*StringSet).Sorted() {
			syncBlocks.Add(blockhash)
		}
	}

	return
}

// Gc the blocks of the seg
func tryGCSegBlocks(scoreSeg ScoreSeg, deviceIdLocal string, blocktype string) (emptied bool, err error) {
	// get blockList
	typeBlocks, ok := META_CACHE.Get(deviceIdLocal)
	if !ok {
		return
	}
	blockList, ok := typeBlocks.Get(blocktype)
	if !ok {
		return
	}

	for i := blockList.Range(scoreSeg.From, scoreSeg.To); i.Next(); {
		for _, blockhash := range i.Value().(*StringSet).Sorted() {
			blockInfo := NewBlockInfo(deviceIdLocal, blocktype, blockhash)
			// try gc
			replicated, err := tryGCBlock(blockInfo)
			if err != nil {
				return false, err
			}
			if !replicated {
				return false, nil
			}

			// sleep 0.1ms
			time.Sleep(1e5)
		}
	}
	return true, nil
}

// Try GC
func tryGCBlock(blockInfo BlockInfo) (replicated bool, err error) {
	logger.Debug("Try rebalance gc block %s", blockInfo)

	if DOMAIN_MAP.IsLocalNodeRemoved() {
		err = fmt.Errorf("No rebalance gc when local node is removed.")
		return
	}

	// throttle
	throttle("tryGCBlock", blockInfo.Device)

	nodes, err := DOMAIN_RING.GetReplicaNodes(blockInfo.Hash, REPLICA, false)
	if err != nil {
		return
	}

	if !canRebalanceGC(blockInfo, nodes) {
		logger.Debug("Can not rebalance gc block %s", blockInfo)
		return false, nil
	}

	// remove local block
	blockPath, err := getBlockPath(blockInfo)
	if err != nil {
		// not exist, remove from META_CACHE
		META_CACHE.DelBlockhash(blockInfo)
		return
	}
	if ExistPath(blockPath) {
		err := os.Remove(blockPath)
		if err != nil {
			logger.Error("Failed to remove rebalance gc block %s, error: %s", blockInfo, err.Error())
			return false, err
		}
	}

	// remove META_CACHE
	META_CACHE.DelBlockhash(blockInfo)

	logger.Info("Rebalance GCed block %s", blockInfo)

	MONITOR.SetVariable("M:REB:RebalanceGC", 1)

	return true, nil
}

// Check rebalance gc status in hashring
func canRebalanceGC(blockInfo BlockInfo, nodes map[string]string) (can bool) {
	// check if on down node
	for nodeIp := range nodes {
		if !DOMAIN_MAP.IsNodeUp(nodeIp) {
			return false
		}
	}

	// check block replica
	taskNum := len(nodes)
	blockStatusChan := make(chan BlockStatusResult, taskNum)
	for nodeIp, deviceId := range nodes {
		blockInfoRemote := NewBlockInfo(deviceId, blockInfo.Type, blockInfo.Hash)
		go queryBlockStatus(blockStatusChan, nodeIp, blockInfoRemote)
	}

	// wait all finished
	for {
		select {
		case result := <-blockStatusChan:

			if result.status != STATUS_OK {
				// miss block
				logger.Debug("Miss block %s, node: %s, device: %s", blockInfo, result.nodeIp, result.deviceId)
				return false
			}
			taskNum -= 1
			if taskNum == 0 {
				// all finished
				return true
			}
		}
		time.Sleep(1e8)
	}
	return false
}
