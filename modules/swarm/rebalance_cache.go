package swarm

import (
	"bytes"
	"fmt"
	"hash"
	"hash/crc32"
	"sync"
	"time"
)

//------------------------------------------------------------------------------
// ScoreSeg
//------------------------------------------------------------------------------

// A range in the ring, from From(include) and to To(not include)
type ScoreSeg struct {
	From uint32
	To   uint32
}

func NewScoreSeg(score uint32) (this ScoreSeg, err error) {
	boundaryTick, err := DOMAIN_RING.GetNextTick(score)
	if err != nil {
		return
	}
	this = ScoreSeg{
		From: score,
		To:   boundaryTick.Score,
	}
	return
}

//------------------------------------------------------------------------------
// Digest
//------------------------------------------------------------------------------

// Represent hash digest of blocks in a branch
type Digest struct {
	lock      *sync.Mutex
	Hash      string
	Expired   bool
	Obsolete  bool
	Timestamp time.Time // timestamp of last modify
}

func (this *Digest) String() (s string) {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	buffer.WriteString("Hash")
	buffer.WriteString(": ")
	buffer.WriteString(this.Hash)
	buffer.WriteString(",")
	buffer.WriteString("Expired")
	buffer.WriteString(": ")
	buffer.WriteString(fmt.Sprintf("%v", this.Expired))
	buffer.WriteString(",")
	buffer.WriteString("Obsolete")
	buffer.WriteString(": ")
	buffer.WriteString(fmt.Sprintf("%v", this.Obsolete))
	buffer.WriteString(",")
	buffer.WriteString("Timestamp")
	buffer.WriteString(": ")
	buffer.WriteString(fmt.Sprintf("%v", this.Timestamp))
	buffer.WriteString(",")
	buffer.WriteString("}")
	return buffer.String()
}

func NewDigest() (this *Digest) {
	this = &Digest{
		lock:      new(sync.Mutex),
		Hash:      "",
		Expired:   true,
		Obsolete:  false,
		Timestamp: time.Now(),
	}
	return
}

func (this *Digest) Expire() {
	this.lock.Lock()
	defer this.lock.Unlock()

	if !this.Expired {
		this.Expired = true
	}
	this.Obsolete = false
	this.Timestamp = time.Now()
}

func (this *Digest) Set(hash string, timestamp time.Time) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.Hash = hash
	if this.Expired && this.Timestamp == timestamp {
		// check timestamp to guarantee branch not changed
		this.Expired = false
	}
	this.Obsolete = false
}

//------------------------------------------------------------------------------
// BranchDigest
//------------------------------------------------------------------------------

type BranchDigest struct {
	lock *sync.RWMutex
	Data map[uint32]*Digest // {branch: Digest}
	Type int
}

func NewBranchDigest(digesttype int) (this *BranchDigest) {
	this = &BranchDigest{
		lock: new(sync.RWMutex),
		Data: make(map[uint32]*Digest),
		Type: digesttype,
	}

	for branch := uint32(0); branch < 1>>16; branch++ {
		this.Data[branch] = NewDigest()
	}
	return
}

func (this *BranchDigest) Get(branch uint32) (digest *Digest, ok bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	digest, ok = this.Data[branch]
	return
}

func (this *BranchDigest) Set(branch uint32, digest *Digest) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.Data[branch] = digest
}

func (this *BranchDigest) Delete(branch uint32) {
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.Data, branch)
}

func (this *BranchDigest) String() (s string) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	var buffer bytes.Buffer
	buffer.WriteString("{")
	for branch, digest := range this.Data {
		buffer.WriteString(fmt.Sprintf("0x%x", branch))
		buffer.WriteString(": ")
		buffer.WriteString(digest.String())
		buffer.WriteString(",")
	}
	buffer.WriteString("}")
	return buffer.String()
}

func (this *BranchDigest) Expire(blockInfo BlockInfo) {
	branch, err := GetBlockBranch(blockInfo.Hash, this.Type)
	if err != nil {
		return
	}

	digest, ok := this.Get(branch)
	if !ok {
		// add new
		digest = NewDigest()
		this.Set(branch, digest)
	}

	digest.Expire()
}

// Branch is bucket of blocks in a ring seg
func GetBlockBranch(blockhash string, digesttype int) (branch uint32, err error) {
	hashBytes, err := HashHexToBytes(blockhash, HASH_HEX_SIZE)
	if err != nil {
		return
	}

	// NOTE: since block score is grouped in ring, to avoid branch grouped too, 
	// which will cause branch blocks not sparse evenly while ring spot 
	// increases, branch can not be related to block score.
	// Choose the first 4 bytes for branch
	if digesttype == 1 {
		branch = uint32(hashBytes[0])<<8 | uint32(hashBytes[1])
	} else {
		branch = uint32(hashBytes[2])<<8 | uint32(hashBytes[3])
	}
	return
}

type ScanHash struct {
	Hash      hash.Hash
	Timestamp time.Time // first timestamp of digest when scan
}

// If blocks increase fastly, a lot of branches will be expired, calc hash would 
// be a burden. To keep time under control, use faster hash crc32 instead of 
// sha1, with 10 times performance improvement. crc32 can easily handle 3 
// million blockhashes per second on my cheap box.
// Choose a parallele digest algorithm if performance is still a problem.
func NewScanHash(timestamp time.Time) (this ScanHash) {
	return ScanHash{
		Hash:      crc32.NewIEEE(),
		Timestamp: timestamp,
	}
}

// Scan whole seg, and cal branch hashes
// It is critical to limit the seg size to avoid QUERY_REBALANCE_CACHE_TIMEOUT
// Ring spots should be choosen carefully based on the major block size.
func (this *BranchDigest) Calc(deviceId string, scoreSeg ScoreSeg, blocktype string, digesttype int) (err error) {
	// get blockList
	typeBlocks, ok := META_CACHE.Get(deviceId)
	if !ok {
		// empty
		return
	}
	blockList, ok := typeBlocks.Get(blocktype)
	if !ok {
		// empty
		return
	}

	scanHashes := make(map[uint32]ScanHash) // {branch: ScanHash}

	// used to find obsolete digest
	calcTimestamp := time.Now()

	// scan seg exist blocks
	for i := blockList.Range(scoreSeg.From, scoreSeg.To); i.Next(); {
		// because block is ordered in ring by score
		// it is safe to calc digest by blockList range
		for _, blockhash := range i.Value().(*StringSet).Sorted() {
			// get branch
			branch, err := GetBlockBranch(blockhash, digesttype)
			if err != nil {
				continue
			}

			// skip unexpired branch
			digest, ok := this.Get(branch)
			if !ok {
				// add new
				digest = NewDigest()
				this.Set(branch, digest)
			}
			if !digest.Expired {
				continue
			}

			// get hash object
			scanHash, ok := scanHashes[branch]
			if !ok {
				// NOTE: branch MAY change after get digest, that will make the 
				// branch unexpired(actually it is expired when we set hash).
				// But it unlikely happens, and rebalance can endure it, and 
				// the next hit change will fix it, so just ignore it.
				scanHash = NewScanHash(digest.Timestamp)
				scanHashes[branch] = scanHash
			}

			// accumulate hash
			scanHash.Hash.Write([]byte(blockhash))
		}
	}

	// calc expired hashes
	for branch, scanHash := range scanHashes {
		digest, ok := this.Get(branch)
		if !ok {
			continue
		}
		// timestamp is used to guard branch change when scan
		digest.Set(HashBytesToHex(scanHash.Hash.Sum(nil)), scanHash.Timestamp)
	}

	// set obsolete branches
	for _, digest := range this.Data {
		if digest.Expired && digest.Timestamp.Before(calcTimestamp) {
			digest.Obsolete = true
		}
	}

	return
}

// Get digests of all branches after last calc
func (this *BranchDigest) GetBranches(deviceId string, scoreSeg ScoreSeg, blocktype string) (branches Branches) {
	branches = make(Branches)

	for branch, digest := range this.Data {
		if digest.Obsolete {
			continue
		}
		branches[branch] = digest.Hash
	}
	return
}

//------------------------------------------------------------------------------
// RebalanceDigest
//------------------------------------------------------------------------------

type RebalanceDigest struct {
	Digest1 *BranchDigest
	Digest2 *BranchDigest
}

func (this *RebalanceDigest) String() (s string) {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	buffer.WriteString("Digest1")
	buffer.WriteString(": ")
	buffer.WriteString(this.Digest1.String())
	buffer.WriteString(",")
	buffer.WriteString("Digest2")
	buffer.WriteString(": ")
	buffer.WriteString(this.Digest2.String())
	buffer.WriteString(",")
	buffer.WriteString("}")
	return buffer.String()
}

func NewRebalanceDigest() (this *RebalanceDigest) {
	return &RebalanceDigest{
		Digest1: NewBranchDigest(1),
		Digest2: NewBranchDigest(2),
	}
}

func (this *RebalanceDigest) Expire(blockInfo BlockInfo) {
	this.Digest1.Expire(blockInfo)
	this.Digest2.Expire(blockInfo)
}

//------------------------------------------------------------------------------
// TypeDigest
//------------------------------------------------------------------------------

type TypeDigest struct {
	lock *sync.RWMutex
	Data map[string]*RebalanceDigest // {blocktype: RebalanceDigest}
}

func NewTypeDigest() (this *TypeDigest) {
	this = &TypeDigest{
		lock: new(sync.RWMutex),
		Data: make(map[string]*RebalanceDigest),
	}

	for _, blocktype := range AVAILABLE_BLOCK_TYPES {
		this.Data[blocktype] = NewRebalanceDigest()
	}

	return
}

func (this *TypeDigest) Get(blocktype string) (rebalanceDigest *RebalanceDigest, ok bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	rebalanceDigest, ok = this.Data[blocktype]
	return
}

func (this *TypeDigest) Set(blocktype string, rebalanceDigest *RebalanceDigest) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.Data[blocktype] = rebalanceDigest
}

func (this *TypeDigest) Delete(blocktype string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.Data, blocktype)
}

func (this *TypeDigest) String() (s string) {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	for blocktype, rebalanceDigest := range this.Data {
		buffer.WriteString(blocktype)
		buffer.WriteString(": ")
		buffer.WriteString(rebalanceDigest.String())
		buffer.WriteString(",")
	}
	buffer.WriteString("}")
	return buffer.String()
}

func (this *TypeDigest) Expire(blockInfo BlockInfo) {
	rebalanceDigest, ok := this.Get(blockInfo.Type)
	if !ok {
		// add new scoreSeg
		rebalanceDigest = NewRebalanceDigest()
		this.Set(blockInfo.Type, rebalanceDigest)
	}

	rebalanceDigest.Expire(blockInfo)
}

//------------------------------------------------------------------------------
// RebalanceSeg
//------------------------------------------------------------------------------

type RebalanceSeg struct {
	lock       *sync.RWMutex
	RepairSegs map[ScoreSeg]*TypeDigest // {scoreSeg: TypeDigest}
	GcSegs     *ScoreSegSet             // {scoreSeg: nil}
}

func NewRebalanceSeg(deviceId string) (this *RebalanceSeg) {
	// data
	data := make(map[ScoreSeg]*TypeDigest)
	rebalanceCacheSegs, err := this.getRebalanceCacheSegs(deviceId)
	if err != nil {
		rebalanceCacheSegs = NewScoreSegSet()
	}
	for scoreSeg := range rebalanceCacheSegs.Data {
		data[scoreSeg] = NewTypeDigest()
	}

	this = &RebalanceSeg{
		lock:       new(sync.RWMutex),
		RepairSegs: data,
	}

	// gcSegs
	gcSegs, err := this.getGcSegs(deviceId)
	if err != nil {
		gcSegs = NewScoreSegSet()
	}
	this.GcSegs = gcSegs

	return
}

func (this *RebalanceSeg) Get(scoreSeg ScoreSeg) (typeDigest *TypeDigest, ok bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	typeDigest, ok = this.RepairSegs[scoreSeg]
	return
}

func (this *RebalanceSeg) Set(scoreSeg ScoreSeg, typeDigest *TypeDigest) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.RepairSegs[scoreSeg] = typeDigest
}

func (this *RebalanceSeg) Delete(scoreSeg ScoreSeg) {
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.RepairSegs, scoreSeg)
}

func (this *RebalanceSeg) String() (s string) {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	for scoreSeg, typeDigest := range this.RepairSegs {
		buffer.WriteString(fmt.Sprintf("%#v", scoreSeg))
		buffer.WriteString(": ")
		buffer.WriteString(typeDigest.String())
		buffer.WriteString(",")
	}
	buffer.WriteString("}")
	return buffer.String()
}

func (this *RebalanceSeg) Expire(blockInfo BlockInfo) {
	primaryTick, err := DOMAIN_RING.GetPrimaryTick(blockInfo.Hash)
	if err != nil {
		logger.Debug("Invalid DOMAIN_RING, error: %s", err.Error())
		return
	}

	scoreSeg, err := NewScoreSeg(primaryTick.Score)
	if err != nil {
		logger.Debug("Failed NewScoreSeg, error: %s", err.Error())
		return
	}

	typeDigest, ok := this.Get(scoreSeg)
	if !ok {
		// not rebalance sync set
		return
	}

	typeDigest.Expire(blockInfo)
}

func (this *RebalanceSeg) Update(deviceId string) {
	// be care not call functions need lock
	this.lock.Lock()
	defer this.lock.Unlock()

	// use new scoreSegs
	rebalanceCacheSegs, err := this.getRebalanceCacheSegs(deviceId)
	if err != nil {
		logger.Debug("Failed update RebalanceSeg, error: %s", err.Error())
		// don't empty whole cache, since domain is expected to be repaired
		return
	}

	for scoreSeg := range this.RepairSegs {
		if !rebalanceCacheSegs.Contains(scoreSeg) {
			// delete obsolete scoreSeg
			delete(this.RepairSegs, scoreSeg)
		}
	}

	for scoreSeg := range rebalanceCacheSegs.Data {
		if _, ok := this.RepairSegs[scoreSeg]; !ok {
			// add new scoreSeg
			this.RepairSegs[scoreSeg] = NewTypeDigest()
		}
		// keep exist segs unchanged
	}

	// update gcSegs
	gcSegs, err := this.getGcSegs(deviceId)
	if err != nil {
		gcSegs = NewScoreSegSet()
	}
	this.GcSegs = gcSegs
}

// Get rebalance cache segs of a local device
// Include local primary segs and all segs of next 2 nodes, which are replicas 
// of remote devices' segs
func (this *RebalanceSeg) getRebalanceCacheSegs(deviceIdLocal string) (rebalanceCacheSegs *ScoreSegSet, err error) {
	// init
	rebalanceCacheSegs = NewScoreSegSet()

	// get device weight
	deviceState, ok := LOCAL_DEVICES.GetState(deviceIdLocal)
	if !ok {
		err = fmt.Errorf("Invalid DOMAIN_MAP to get deviceId %s", deviceIdLocal)
		return
	}
	if deviceState.Status == DEVICE_STATUS_REMOVED {
		// skip removed device
		err = fmt.Errorf("Removed device %s", deviceIdLocal)
		return
	}

	// save primary segs
	for _, primaryScore := range GetNodeScores(deviceIdLocal, DOMAIN_RING_SPOTS, deviceState.Weight) {
		scoreSeg, err := NewScoreSeg(primaryScore)
		if err != nil {
			return nil, err
		}
		rebalanceCacheSegs.Add(scoreSeg)
	}

	// scan remote nodes, find local nodes as a replica
	for nodeIp, nodeState := range DOMAIN_MAP.Data {
		if nodeIp == NODE_ADDRESS {
			// skip local node
			continue
		}

		for deviceIdRemote, weight := range nodeState.Devices.Data {
			for _, primaryScoreRemote := range GetNodeScores(deviceIdRemote, DOMAIN_RING_SPOTS, *weight) {
				ticks, _, err := DOMAIN_RING.GetReplicaTicks(0, NewTick(deviceIdRemote, primaryScoreRemote), REPLICA, false, false)
				if err != nil {
					return nil, err
				}
				for tick := range ticks {
					if tick.Name == deviceIdLocal {
						// found
						scoreSeg, err := NewScoreSeg(primaryScoreRemote)
						if err != nil {
							return nil, err
						}
						rebalanceCacheSegs.Add(scoreSeg)
					}
				}
			}
		}
	}

	return
}

// Get rebalance gc segs of a local device
func (this *RebalanceSeg) getGcSegs(deviceId string) (gcSegs *ScoreSegSet, err error) {
	// init
	gcSegs = NewScoreSegSet()

	for _, tick := range DOMAIN_RING.ticks {
		scoreSeg, err := NewScoreSeg(tick.Score)
		if err != nil {
			return gcSegs, err
		}
		if _, ok := this.RepairSegs[scoreSeg]; ok {
			// not gc seg
			continue
		}
		gcSegs.Add(scoreSeg)
	}
	return
}

//------------------------------------------------------------------------------
// RebalanceCache
//------------------------------------------------------------------------------

// {deviceid: {scoreseg: {blocktype: {Digest1: BranchDigest, Digest2: BranchDigest, }}}}
type RebalanceCache struct {
	lock *sync.RWMutex
	Data map[string]*RebalanceSeg // {deviceid: RebalanceSeg}
	Out  *StringSet               // rebalance out node
}

func NewRebalanceCache() (this RebalanceCache) {
	this = RebalanceCache{
		lock: new(sync.RWMutex),
		Data: make(map[string]*RebalanceSeg),
		Out:  NewStringSet(),
	}

	for deviceId := range LOCAL_DEVICES.Data {
		this.Data[deviceId] = NewRebalanceSeg(deviceId)
	}
	return
}

func (this RebalanceCache) Get(deviceId string) (rebalanceSeg *RebalanceSeg, ok bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	rebalanceSeg, ok = this.Data[deviceId]
	return
}

func (this RebalanceCache) Set(deviceId string, rebalanceSeg *RebalanceSeg) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.Data[deviceId] = rebalanceSeg
}

func (this RebalanceCache) Delete(deviceId string) {
	this.lock.Lock()
	defer this.lock.Unlock()

	delete(this.Data, deviceId)
}

func (this RebalanceCache) String() (s string) {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	for deviceId, rebalanceSeg := range this.Data {
		buffer.WriteString(deviceId)
		buffer.WriteString(": ")
		buffer.WriteString(rebalanceSeg.String())
		buffer.WriteString(",")
	}
	buffer.WriteString("}")
	return buffer.String()
}

// Expire blockinfo in RebalanceCache
func (this RebalanceCache) Expire(blockInfo BlockInfo) {
	// verify device is local
	if !LOCAL_DEVICES.IsLocalDevice(blockInfo.Device) {
		logger.Debug("Invalid DOMAIN_MAP to get deviceId %s", blockInfo.Device)
		return
	}

	// get rebalanceSeg
	rebalanceSeg, ok := this.Get(blockInfo.Device)
	if !ok {
		rebalanceSeg = NewRebalanceSeg(blockInfo.Device)
		this.Set(blockInfo.Device, rebalanceSeg)
	}
	rebalanceSeg.Expire(blockInfo)
}

type Branches map[uint32]string // {branch: hash}

type SyncDigest struct {
	Branches1 Branches
	Branches2 Branches
}

func (this RebalanceCache) GetSyncDigest(deviceId string, scoreSeg ScoreSeg, blocktype string) (syncDigest SyncDigest, err error) {
	// record start time
	start := time.Now()

	// get Digests
	rebalanceSeg, ok := this.Get(deviceId)
	if !ok {
		err = fmt.Errorf("Failed to get device %s from RebalanceCache in GetSyncDigest", deviceId)
		return
	}

	typeDigest, ok := rebalanceSeg.Get(scoreSeg)
	if !ok {
		err = SwarmErrorf(ERROR_INVALID_SCORESEG, "Failed to get device %s scoreSeg %#v from RebalanceCache", deviceId, scoreSeg)
		return
	}

	rebalanceDigest, ok := typeDigest.Get(blocktype)
	if !ok {
		err = fmt.Errorf("Failed to get device %s scoreSeg %#v blocktype %s from RebalanceCache", deviceId, scoreSeg, blocktype)
		return
	}

	// preprocess
	err = rebalanceDigest.Digest1.Calc(deviceId, scoreSeg, blocktype, 1)
	if err != nil {
		return
	}
	err = rebalanceDigest.Digest2.Calc(deviceId, scoreSeg, blocktype, 2)
	if err != nil {
		return
	}

	// get hashes
	syncDigest.Branches1 = rebalanceDigest.Digest1.GetBranches(deviceId, scoreSeg, blocktype)
	syncDigest.Branches2 = rebalanceDigest.Digest2.GetBranches(deviceId, scoreSeg, blocktype)

	end := time.Now()
	if end.Sub(start) > QUERY_REBALANCE_CACHE_TIMEOUT {
		// alarm
		ALARM_MANAGER.Add(ALARM_REBALANCE_CACHE_TIMEOUT, fmt.Errorf(""), "Get rebalance cache timeout, device %s", deviceId)
	}

	return
}

// Update rebalance cache when domain ring changed
func (this RebalanceCache) Update() {
	// be care not call functions need lock
	this.lock.Lock()
	defer this.lock.Unlock()

	if _, ok := DOMAIN_MAP.Get(NODE_ADDRESS); !ok {
		// should never happen
		for deviceId := range this.Data {
			// empty
			delete(this.Data, deviceId)
		}
		return
	}

	for deviceId := range this.Data {
		if !LOCAL_DEVICES.IsLocalDevice(deviceId) {
			// delete obsolete device not exist on local
			delete(this.Data, deviceId)
		}
	}

	for deviceId := range LOCAL_DEVICES.Data {
		rebalanceSeg, ok := this.Data[deviceId]
		if !ok {
			// add new device
			this.Data[deviceId] = NewRebalanceSeg(deviceId)
			continue
		}
		rebalanceSeg.Update(deviceId)
	}
}

func (this RebalanceCache) GetRepairScoreSegs(deviceId string) (scoreSegs map[ScoreSeg]*TypeDigest, err error) {
	// get Digests
	rebalanceSeg, ok := this.Get(deviceId)
	if !ok {
		err = fmt.Errorf("Failed to get device %s from RebalanceCache in GetRepairScoreSegs", deviceId)
		return
	}

	scoreSegs = rebalanceSeg.RepairSegs
	return
}

func (this RebalanceCache) GetGcSegs(deviceId string) (gcSegs *ScoreSegSet, err error) {
	// get Digests
	rebalanceSeg, ok := this.Get(deviceId)
	if !ok {
		err = fmt.Errorf("Failed to get device %s from RebalanceCache in GetGcSegs", deviceId)
		return
	}

	gcSegs = rebalanceSeg.GcSegs
	return
}
