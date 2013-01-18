package swarm

import (
	"fmt"
	"time"
)

//------------------------------------------------------------------------------
// Rebalancer
//------------------------------------------------------------------------------

/*
Periodically tranverse REBALANCE_CACHE, find blocks need to be repaired by remote 
node, and notify remote node of blocks they should have.

The term rebalance: 
In other distributed systems, rebalance is process of migrating data among nodes 
to take into account node utilization of disk space and other resources. 
We use it to discribe the process of repairing and removing blocks to satisfy 
the replica policy.
*/
type Rebalancer struct {
	WorkerBase
	watchdog  *Watchdog
	semaphore *Semaphore
}

// Implement interface Restartable
func (this *Rebalancer) Name() string {
	return "Rebalancer"
}

// Implement interface Restartable
func (this *Rebalancer) Start(r Restartable, e interface{}) {
	defer func() {
		e := recover()
		this.OnPanic(this, this.watchdog, e)
	}()

	logger.Critical("Rebalancer started.")

	for {
		time.Sleep(randInterval(REBALANCE_INTERVAL))

		// wait if full
		this.semaphore.Acquire()
		go this.doRebalance()
	}

	logger.Critical("Rebalancer stopped.")
}

// TODO: when domain map changed, update cache, cancel rebalance process
// TODO: prevent timeout when requestRebalanceCache
// TODO: record and alarm continuous failed node
// TODO: limit push block amount
// Push rebalance message.
// Notify remote nodes of blocks they should have, remote nodes should check 
// blocks to find what they are missing and request them from this node.
func (this *Rebalancer) doRebalance() {
	defer func() {
		time.Sleep(randInterval(REBALANCE_INTERVAL))
		this.semaphore.Release()
	}()

	if !META_CACHE.Ready {
		// not ready
		return
	}

	// throttle
	throttle("doRebalance", "")

	// pick seg
	// include online and offline devices
	deviceIdLocal, scoreSeg, blocktype, err := randomRebalanceSeg()
	if err != nil {
		return
	}

	// get primaryTick
	primaryTick, err := DOMAIN_RING.GetPrimaryTickByScore(scoreSeg.From)
	if err != nil {
		return
	}

	// sync with other replicas
	_, nodes, err := DOMAIN_RING.GetReplicaTicks(0, primaryTick, REPLICA, false, false)
	if err != nil {
		return
	}
	for nodeIpRemote, deviceIdRemote := range nodes {
		if nodeIpRemote == NODE_ADDRESS && deviceIdRemote == deviceIdLocal {
			// skip local device
			continue
		}
		if !DOMAIN_MAP.IsNodeUp(nodeIpRemote) {
			// skip down node
			continue
		}
		_, err := syncRemote(scoreSeg, deviceIdLocal, nodeIpRemote, deviceIdRemote, blocktype)
		if err != nil {
			logger.Debug("Failed syncRemote %s device %s, error: %s", nodeIpRemote, deviceIdRemote, err.Error())
		}
	}
}

// Pick a random available device, take weight into account
func randomDevice() (deviceIdLocal string, err error) {
	devices := make([]string, 0)
	for deviceId, deviceState := range LOCAL_DEVICES.Data {
		for i := 0; i < deviceState.Weight; i++ {
			devices = append(devices, deviceId)
		}
	}

	deviceIdLocal, err = ChoiceString(devices)
	if err != nil {
		return
	}
	return
}

// Pick a random reblance seg
func randomRebalanceSeg() (deviceIdLocal string, scoreSeg ScoreSeg, blocktype string, err error) {
	deviceIdLocal, err = randomDevice()
	if err != nil {
		return
	}

	// get seg
	scoreSegs, err := REBALANCE_CACHE.GetRepairScoreSegs(deviceIdLocal)
	if err != nil {
		return
	}

	keys := make([]ScoreSeg, 0)
	for scoreSeg := range scoreSegs {
		keys = append(keys, scoreSeg)
	}

	scoreSeg, err = ChoiceScoreSeg(keys)
	if err != nil {
		return
	}

	blocktype, _ = ChoiceString(AVAILABLE_BLOCK_TYPES)
	return
}

// Sync blocks with remote node
func syncRemote(scoreSeg ScoreSeg, deviceIdLocal string, nodeIpRemote string, deviceIdRemote string, blocktype string) (count int, err error) {
	// some check
	if err = canPushRebalance(nodeIpRemote); err != nil {
		return
	}

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
	if blockList.Data.Length == 0 {
		// empty local blocks
		return
	}

	// get remote SyncDigest
	remoteSyncDigest, err := requestRebalanceCache(nodeIpRemote, deviceIdRemote, scoreSeg, blocktype)
	if err != nil {
		return
	}

	// get local SyncDigest
	localSyncDigest, err := REBALANCE_CACHE.GetSyncDigest(deviceIdLocal, scoreSeg, blocktype)
	if err != nil {
		checkUnmatchScoreSeg(nodeIpRemote, err)
		return
	}

	// get syncBlocks to push
	syncBlocks, err := getSyncBlocks(scoreSeg, deviceIdLocal, blocktype, localSyncDigest, remoteSyncDigest)
	if err != nil {
		return
	}
	// set count
	count = syncBlocks.Len()

	// push
	err = requestRebalancePush(deviceIdLocal, nodeIpRemote, deviceIdRemote, blocktype, syncBlocks)
	if err != nil {
		return
	}

	return
}

// Get local branches diff from remote
func diffBranches(localBranches Branches, remoteBranches Branches) (branches *Uint32Set) {
	branches = NewUint32Set() // {branch: nil}

	for branch, localHash := range localBranches {
		remoteHash, ok := remoteBranches[branch]
		if !ok || localHash != remoteHash {
			branches.Add(branch)
		}
	}
	return
}

// Get blocks of branches
func getBlocksOfBranches(scoreSeg ScoreSeg, deviceId string, blocktype string, digesttype int, branches *Uint32Set) (branchBlocks *StringSet, err error) {
	// init
	branchBlocks = NewStringSet()

	if branches.Len() == 0 {
		// empty diff
		return
	}

	// get blockList
	typeBlocks, ok := META_CACHE.Get(deviceId)
	if !ok {
		//empty
		return
	}
	blockList, ok := typeBlocks.Get(blocktype)
	if !ok {
		//empty
		return
	}

	for i := blockList.Range(scoreSeg.From, scoreSeg.To); i.Next(); {
		for _, blockhash := range i.Value().(*StringSet).Sorted() {
			// get branch
			blockBranch, err := GetBlockBranch(blockhash, digesttype)
			if err != nil {
				continue
			}
			if branches.Contains(blockBranch) {
				// found
				branchBlocks.Add(blockhash)
			}
		}
	}
	return
}

/*
Get intersection blocks of branchset 1 & 2

This algorithm is optimized for finding small diffs between huge bases. 
Percent of changed branch is exponential for sync blocks, limit changed branch is critical. 
When blocks in a branch are in high level, too many changed branches produce huge candidate blocks, and will make big intersection.
For example, if each branch contains 100 blocks: 
    (candidates of branchset1 is distributed evenly in branchset2)
    10% change branches is 6,553 (65536*0.1), total candidates is 655,300(6553*100), the intersection is 65,530 (655300*0.1).
    1% change branches is 655 (65536*0.01), total candidates is 65,500(655*100), the intersection is 655 (65500*0.01).
    Percent of changed branch is only related to amount of branches, 10% is about 6,553 blocks.

Explain: 
Calc the digest of branch is a dimension reduction procedure. With two or more digests of different dimension, we can rebuild an enveloping of the origin, which discribe what MAY missing in the origin. 
The process is to compare with local to get the MAY missing branches (itmes of these branches is bigger then real ones).
The result items is items MAY missing in all dimensions, which is still bigger then real ones but is expected much smaller than items of each dimensions.
After get result items, just send them to origin. 
*/
func getSyncBlocks(scoreSeg ScoreSeg, deviceId string, blocktype string, localSyncDigest SyncDigest, remoteSyncDigest SyncDigest) (syncBlocks *StringSet, err error) {
	syncBlocks = NewStringSet()

	// get local branches diff from remote
	branches1 := diffBranches(localSyncDigest.Branches1, remoteSyncDigest.Branches1)
	branches2 := diffBranches(localSyncDigest.Branches2, remoteSyncDigest.Branches2)

	branchBlocks1, err := getBlocksOfBranches(scoreSeg, deviceId, blocktype, 1, branches1)
	if err != nil {
		return
	}
	branchBlocks2, err := getBlocksOfBranches(scoreSeg, deviceId, blocktype, 2, branches2)
	if err != nil {
		return
	}

	// calc intersection
	for blockhash := range branchBlocks1.Data {
		if branchBlocks2.Contains(blockhash) {
			syncBlocks.Add(blockhash)
		}
	}
	for blockhash := range branchBlocks2.Data {
		if branchBlocks1.Contains(blockhash) {
			syncBlocks.Add(blockhash)
		}
	}
	return
}

// Do admin rebalance
func adminRebalance(nodeIp string, deviceIdLocal string) (err error) {
	logger.Info("Admin rebalance to node %s from local device %s", nodeIp, deviceIdLocal)

	// return error when node is down
	if !DOMAIN_MAP.IsNodeUp(nodeIp) {
		err = fmt.Errorf("Can not rebalance down node %s.", nodeIp)
		return
	}

	for _, blocktype := range AVAILABLE_BLOCK_TYPES {
		scoreSegs, err := REBALANCE_CACHE.GetRepairScoreSegs(deviceIdLocal)
		if err != nil {
			continue
		}

		for scoreSeg := range scoreSegs {
			// get primaryTick
			primaryTick, err := DOMAIN_RING.GetPrimaryTickByScore(scoreSeg.From)
			if err != nil {
				continue
			}

			// sync with other replicas
			_, nodes, err := DOMAIN_RING.GetReplicaTicks(0, primaryTick, REPLICA, false, false)
			if err != nil {
				continue
			}

			count := 30
			for nodeIpRemote, deviceIdRemote := range nodes {
				if nodeIpRemote != nodeIp {
					// not target node
					continue
				}
				if nodeIpRemote == NODE_ADDRESS && deviceIdRemote == deviceIdLocal {
					// skip local device
					continue
				}

				// wait
				waitPushRebalance(nodeIpRemote, count)

				count, err = syncRemote(scoreSeg, deviceIdLocal, nodeIp, deviceIdRemote, blocktype)
				if err != nil {
					logger.Debug("Failed syncRemote %s device %s, error: %s", nodeIp, deviceIdRemote, err.Error())
					continue
				}

				if count > 0 {
					logger.Debug("Push %d blocks from %#v to node %s device %s", count, scoreSeg, nodeIp, deviceIdRemote)
				}
			}
		}
	}
	return nil
}

// Wait for remote to grant for rebalance with count seconds timeout 
func waitPushRebalance(nodeIpRemote string, count int) {
	start := time.Now()
	for err := canPushRebalance(nodeIpRemote); err != nil; {
		if err.Error() != ADMIN_REBALANCE_DUPLICATED && err.Error() != ADMIN_REBALANCE_EXCEED {
			// other error, just return
			return
		}
		if time.Now().Sub(start) > time.Duration(count)*time.Second {
			return
		}
		time.Sleep(10 * time.Second)
	}
}
