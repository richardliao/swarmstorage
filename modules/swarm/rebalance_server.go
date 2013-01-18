package swarm

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

//------------------------------------------------------------------------------
// RepairNodeStatus
//------------------------------------------------------------------------------

type RepairNodeStatus struct {
	lock   *sync.Mutex
	Start  time.Time // rebalance start time
	Last   time.Time // last active time
	Device string    // current rebalance local device
	Total  int       // total blocks
	Pull   int       // pulled blocks
	Skip   int       // skipped blocks
}

func NewRepairNodeStatus(start time.Time) *RepairNodeStatus {
	return &RepairNodeStatus{
		lock:  new(sync.Mutex),
		Start: start,
		Last:  time.Now(),
	}
}

func (this *RepairNodeStatus) Set(field string, value interface{}) {
	this.lock.Lock()
	defer this.lock.Unlock()

	switch field {
	case "Start":
		this.Start = value.(time.Time)
	case "Last":
		this.Last = value.(time.Time)
	case "Device":
		this.Device = value.(string)
	case "Total":
		this.Total = value.(int)
	case "Pull":
		this.Pull = value.(int)
	case "Skip":
		this.Skip = value.(int)
	}
}

//------------------------------------------------------------------------------
// RepairStatus
//------------------------------------------------------------------------------

type RepairStatus struct {
	lock *sync.RWMutex
	Data map[string]*RepairNodeStatus // {nodeIp: *RepairNodeStatus}
}

func NewRepairStatus() RepairStatus {
	return RepairStatus{
		lock: new(sync.RWMutex),
		Data: make(map[string]*RepairNodeStatus),
	}
}

func (this RepairStatus) Get(nodeIp string) (repairStatus *RepairNodeStatus, ok bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	repairStatus, ok = this.Data[nodeIp]
	return
}

func (this RepairStatus) Set(nodeIp string, repairStatus *RepairNodeStatus) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.Data[nodeIp] = repairStatus
}

func (this RepairStatus) Delete(nodeIp string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.Data, nodeIp)
}

// Global

var REPAIR_STATUS RepairStatus

func init() {
	REPAIR_STATUS = NewRepairStatus()
}

//------------------------------------------------------------------------------
// RebalanceServer
//------------------------------------------------------------------------------

/*
Receive rebalance push messages about blocks record this node should have. 
Find missing blocks and pull them from source node.
*/
type RebalanceServer struct {
	WorkerBase
	watchdog *Watchdog
}

// Implement interface Restartable
func (this *RebalanceServer) Name() string {
	return "RebalanceServer"
}

// Implement interface Restartable
func (this *RebalanceServer) Start(r Restartable, e interface{}) {
	defer func() {
		e := recover()
		this.OnPanic(this, this.watchdog, e)
	}()

	logger.Critical("RebalanceServer started.")

	http.Handle(URL_SWARM_REBALANCE_CACHE, httpHandler(handleRebalanceCache))
	http.Handle(URL_SWARM_REBALANCE_PUSH, httpHandler(handleRebalancePush))
	http.Handle(URL_SWARM_REBALANCE_QUERY, httpHandler(handleRebalanceQuery))
	err := http.ListenAndServe(NODE_ADDRESS+":"+REBALANCE_PORT, nil)
	checkFatal(err)

	logger.Critical("RebalanceServer stopped.")
}

// Handle rebalance request
type RebalanceCacheResponse struct {
	Version    string
	Timestamp  time.Time
	Status     int
	SyncDigest SyncDigest
}

func handleRebalanceCache(writer http.ResponseWriter, req *http.Request) *SwarmError {
	response := RebalanceCacheResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"source", verifyNothing},
		ParamVerifier{"deviceid", verifyDeviceId},
		ParamVerifier{"from", verifyScore},
		ParamVerifier{"to", verifyScore},
		ParamVerifier{"blocktype", verifyBlocktype},
	})
	if swarmError != nil {
		return swarmError
	}
	nodeIp, deviceId, fromStr, toStr, blocktype := params[0], params[1], params[2], params[3], params[4]
	from, _ := strconv.Atoi(fromStr)
	to, _ := strconv.Atoi(toStr)
	scoreSeg := ScoreSeg{
		From: uint32(from),
		To:   uint32(to),
	}

	// verify device is online
	if !LOCAL_DEVICES.IsLocalOnlineDevice(deviceId) {
		return SwarmErrorf(http.StatusInternalServerError, "Invalid device to rebalance")
	}

	// get RebalanceCache
	syncDigest, err := REBALANCE_CACHE.GetSyncDigest(deviceId, scoreSeg, blocktype)
	if err != nil {
		checkUnmatchScoreSeg(nodeIp, err)
		return SwarmErrorf(http.StatusInternalServerError, "Failed get RebalanceCache, error: %s", err.Error())
	}

	// syncDigest is guaranteed valid in REBALANCE_CACHE, it is unneccessary to
	// verify domain version again. It is gossiper's responsibility to notify
	// device changes to correlate nodes. Unmatched param to get syncDigest
	// will return error, which will stop this rebalance attempt.

	response.SyncDigest = syncDigest

	// response
	encoder := gob.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}

	return nil
}

// Gossip notify when score seg not match
func checkUnmatchScoreSeg(nodeIp string, err error) {
	if swarmError, ok := err.(SwarmError); ok && swarmError.Code == ERROR_INVALID_SCORESEG && nodeIp != NODE_ADDRESS {
		// notify gossip local
		GOSSIP_NOTIFIER.Add(nodeIp)
		// notify gossip remote node
		requestGossipNotify(nodeIp)
	}
}

// Handle rebalance request
func handleRebalancePush(writer http.ResponseWriter, req *http.Request) *SwarmError {
	response := BaseResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"source", verifyNothing},
		ParamVerifier{"deviceidlocal", verifyNothing},
		ParamVerifier{"deviceidremote", verifyDeviceId},
		ParamVerifier{"blocktype", verifyBlocktype},
	})
	if swarmError != nil {
		return swarmError
	}
	// EXCHANGE deviceIdRemote, deviceIdLocal
	nodeIp, deviceIdRemote, deviceIdLocal, blocktype := params[0], params[1], params[2], params[3]

	// verify device is online
	if !LOCAL_DEVICES.IsLocalOnlineDevice(deviceIdLocal) {
		return SwarmErrorf(http.StatusInternalServerError, "Invalid device to rebalance")
	}

	// parse request
	decoder := gob.NewDecoder(req.Body)
	syncBlocks := NewStringSet()
	if err := decoder.Decode(syncBlocks); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Decode error: %s", err.Error())
	}

	// do repair
	go repairFromNode(nodeIp, deviceIdRemote, deviceIdLocal, blocktype, syncBlocks)

	// response
	encoder := gob.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}

	return nil
}

// Repair blocks from node
// TODO: stop repair when device offline/removed
func repairFromNode(nodeIp string, deviceIdRemote string, deviceIdLocal string, blocktype string, syncBlocks *StringSet) {
	defer onRecoverablePanic()

	if deviceIdRemote == deviceIdLocal {
		return
	}

	startTime := time.Now()

	// set REPAIR_STATUS
	status := NewRepairNodeStatus(startTime)
	if _, ok := REPAIR_STATUS.Get(nodeIp); !ok {
		REPAIR_STATUS.Set(nodeIp, status)
	}
	defer func() {
		currentStatus, ok := REPAIR_STATUS.Get(nodeIp)
		if !ok || currentStatus.Start != startTime {
			// not belong to this process
			return
		}
		REPAIR_STATUS.Delete(nodeIp)
	}()

	// get total
	status.Set("Total", syncBlocks.Len())

	// set status
	status.Set("Device", deviceIdLocal)

	for blockhash := range syncBlocks.Data {
		// throttle
		throttle("repairFromNode", deviceIdLocal)

		if currentStatus, ok := REPAIR_STATUS.Get(nodeIp); !ok || currentStatus.Start != startTime {
			// this rebalance process should be terminated
			return
		}

		// update last active time
		status.Set("Last", time.Now())

		blockInfoLocal := NewBlockInfo(deviceIdLocal, blocktype, blockhash)
		blockPath, err := getBlockPath(blockInfoLocal)
		if err != nil {
			// TODO: warning
			continue
		}

		if !ExistPath(blockPath) {
			// missing block
			// TODO: throttle
			blockInfoRemote := NewBlockInfo(deviceIdRemote, blocktype, blockhash)
			pullBlock(blockInfoLocal, blockInfoRemote, nodeIp)
			status.Set("Pull", status.Pull+1)
		} else {
			status.Set("Skip", status.Skip+1)
		}
	}
}

// Repair block for rebalance
func pullBlock(blockInfoLocal BlockInfo, blockInfoRemote BlockInfo, nodeIp string) {
	defer onRecoverablePanic()

	logger.Debug("Try repair block %s from node %s", blockInfoLocal, nodeIp)

	// check if block belongs to this node
	//verifyBlockRing(blockInfoLocal.Hash, nodeIp)

	pdata, err := requestReadBlock(nodeIp, blockInfoRemote, false, STATUS_PROXIED)
	if err != nil {
		logger.Error("Error in request read block %s node %s, error: %s", blockInfoRemote, nodeIp, err.Error())
		return
	}

	// verify
	_, err = verifyBlock(blockInfoLocal.Hash, blockInfoLocal.Type, pdata)
	if err != nil {
		ALARM_MANAGER.Add(ALARM_MISMATCH_BLOCKHASH, err, "Mismatch hash, block %s node %s", blockInfoRemote, nodeIp)
		return
	}

	err = writeBlockLocalDevice(blockInfoLocal, pdata)
	if err != nil {
		logger.Error("Failed write block %s, error: %s", blockInfoLocal, err.Error())
		return
	}
	logger.Info("Repaired block %s from node %s", blockInfoLocal, nodeIp)

	MONITOR.SetVariable("M:REB:RepairBlock", 1)
}

// Handle rebalance query request
func handleRebalanceQuery(writer http.ResponseWriter, req *http.Request) *SwarmError {
	response := BaseResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"source", verifyNothing},
	})
	if swarmError != nil {
		return swarmError
	}
	nodeIp := params[0]

	// check rebalance status
	if status, ok := REPAIR_STATUS.Get(nodeIp); ok {
		if time.Now().After(status.Last.Add(MAX_REBALANCE_IDLE)) {
			// delete idle process
			REPAIR_STATUS.Delete(nodeIp)
		} else {
			// a rebalance process running for the same node
			response.Status = STATUS_ERROR
			response.Desc = ADMIN_REBALANCE_DUPLICATED
		}
	}

	// check ongoing rebalance process
	if len(REPAIR_STATUS.Data) > MAX_CONCURRENT_REPAIR {
		response.Status = STATUS_ERROR
		response.Desc = ADMIN_REBALANCE_EXCEED
	}

	// response
	encoder := gob.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}

	return nil
}

// Request rebalance cache
func requestRebalanceCache(nodeIp string, deviceId string, scoreSeg ScoreSeg, blocktype string) (syncDigest SyncDigest, err error) {
	defer onRecoverablePanic()

	if DOMAIN_MAP.IsLocalNodeRemoved() {
		err = fmt.Errorf("No rebalance when local node is removed.")
		return
	}

	query := url.Values{
		"source":    []string{NODE_ADDRESS},
		"deviceid":  []string{deviceId},
		"from":      []string{strconv.Itoa(int(scoreSeg.From))},
		"to":        []string{strconv.Itoa(int(scoreSeg.To))},
		"blocktype": []string{blocktype},
	}

	pdata, err := RequestHttp(nodeIp, REBALANCE_PORT, URL_SWARM_REBALANCE_CACHE, query, "GET", nil, false, QUERY_REBALANCE_CACHE_TIMEOUT, true)
	if err != nil {
		logger.Error("Failed request rebalance cache node %s, error: %s", nodeIp, err.Error())
		return
	}

	response := new(RebalanceCacheResponse)
	if err = gob.NewDecoder(bytes.NewReader(*pdata)).Decode(response); err != nil {
		logger.Error("Failed decode rebalance cache response from node %s, error: %s", nodeIp, err.Error())
		return
	}

	if response.Status != STATUS_OK {
		err = fmt.Errorf("Failed request rebalance cache node %s", nodeIp)
		return
	}

	MONITOR.SetVariable("M:REB:RebalanceCache", 1)

	return response.SyncDigest, nil
}

// Request rebalance
// Remote node will terminate repair process from the same source node, 
// we should check canPushRebalance in advance.
func requestRebalancePush(deviceIdLocal string, nodeIpRemote string, deviceIdRemote string, blocktype string, syncBlocks *StringSet) (err error) {
	defer onRecoverablePanic()

	if deviceIdLocal == deviceIdRemote {
		return
	}

	if DOMAIN_MAP.IsLocalNodeRemoved() {
		err = fmt.Errorf("No rebalance when local node is removed.")
		return
	}

	query := url.Values{
		"source":         []string{NODE_ADDRESS},
		"deviceidlocal":  []string{deviceIdLocal},
		"deviceidremote": []string{deviceIdRemote},
		"blocktype":      []string{blocktype},
	}

	buff := new(bytes.Buffer)
	gobEncoder := gob.NewEncoder(buff)
	err = gobEncoder.Encode(syncBlocks)
	if err != nil {
		return err
	}
	requestData := buff.Bytes()

	pdata, err := RequestHttp(nodeIpRemote, REBALANCE_PORT, URL_SWARM_REBALANCE_PUSH, query, "POST", &requestData, false, 30*time.Second, true)
	if err != nil {
		logger.Error("Failed request rebalance node %s, error: %s", nodeIpRemote, err.Error())
		return err
	}

	response := new(BaseResponse)
	if err := gob.NewDecoder(bytes.NewReader(*pdata)).Decode(response); err != nil {
		logger.Error("Failed decode rebalance response from node %s, error: %s", nodeIpRemote, err.Error())
		return err
	}

	if response.Status != STATUS_OK {
		return fmt.Errorf("Failed request rebalance node %s", nodeIpRemote)
	}

	MONITOR.SetVariable("M:REB:PushRebalance", 1)

	return
}

// Query permission for rebalance
func canPushRebalance(nodeIp string) (err error) {
	defer onRecoverablePanic()

	if DOMAIN_MAP.IsLocalNodeRemoved() {
		err = fmt.Errorf("No rebalance when local node is removed.")
		return
	}

	query := url.Values{
		"source": []string{NODE_ADDRESS},
	}

	pdata, err := RequestHttp(nodeIp, REBALANCE_PORT, URL_SWARM_REBALANCE_QUERY, query, "GET", nil, false, 5*time.Second, true)
	if err != nil {
		logger.Error("Failed query rebalance status from node %s, error: %s", nodeIp, err.Error())
		return err
	}

	response := new(BaseResponse)
	if err := gob.NewDecoder(bytes.NewReader(*pdata)).Decode(response); err != nil {
		logger.Error("Failed decode rebalance response from node %s error: %s", nodeIp, err.Error())
		return err
	}

	if response.Status != STATUS_OK {
		return fmt.Errorf(response.Desc)
	}

	return nil
}
