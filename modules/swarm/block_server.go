package swarm

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

/*
Handoff(not hinted handoff):

Block server should try its best to write local blocks. It should handoff 
writing blocks to other local devices until all failed.

Distributer block server should handoff writing blocks to candidate replicas for 
high availability. 

OSS server just try 3 replica nodes to distribute a block, that seems enough.

The handed off blocks will be rebalanced by repair/gc. After a device is 
removed, a fast repair should be triggered to rebalance handoff blocks(optional).

Handoff work on write/read, not rebalance. Rebalance push to offline disk will be 
rejected. Rebalance gc will skip handed off blocks. 
*/

//------------------------------------------------------------------------------
// BlockServer
//------------------------------------------------------------------------------

type BlockServer struct {
	WorkerBase
	watchdog *Watchdog
}

// Implement interface Restartable
func (this *BlockServer) Name() string {
	return "BlockServer"
}

// Implement interface Restartable
func (this *BlockServer) Start(r Restartable, e interface{}) {
	defer func() {
		e := recover()
		this.OnPanic(this, this.watchdog, e)
	}()

	logger.Critical("BlockServer started.")

	http.Handle(URL_SWARM_REPLICA_BLOCK, httpHandler(handleReplicaBlock))
	http.Handle(URL_SWARM_WRITE_BLOCK, httpHandler(handleWriteBlock))
	http.Handle(URL_SWARM_READ_BLOCK, httpHandler(handleReadBlock))
	http.Handle(URL_SWARM_DEL_BLOCK, httpHandler(handleDelBlock))
	http.Handle(URL_SWARM_BLOCK_STATUS, httpHandler(handleBlockStatus))
	http.Handle(URL_SWARM_BLOCK_EXIST, httpHandler(handleBlockExist))

	err := http.ListenAndServe(NODE_ADDRESS+":"+BLOCK_PORT, nil)
	checkFatal(err)

	logger.Critical("BlockServer stopped.")
}

// Handle replica block
type ReplicaBlockResult struct {
	nodeIp   string
	deviceId string
	status   int
}

// OSS distribute block by request replica block to a block server when handle 
// object writing. It is the block server's responsibility to redistribute 
// block to replicas.
// After verifying block content, the handler try to distribute block to nodes
// by ring. If any failed, retry to distribute block to relay nodes, the number
// of retry nodes can be same as replica or at least failed number.
func handleReplicaBlock(writer http.ResponseWriter, req *http.Request) *SwarmError {
	MONITOR.SetVariable("M:BS:ReplicaBlock", 1)

	// check request
	if req.Method != "POST" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	if req.ContentLength < 1 {
		return SwarmErrorf(http.StatusBadRequest, "Invalid content length: %s", req.ContentLength)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"blocktype", verifyBlocktype},
		ParamVerifier{"blockhash", verifyBlockhash},
		ParamVerifier{"replica", verifyReplica},
	})
	if swarmError != nil {
		return swarmError
	}
	blocktype, blockhash, replicaStr := params[0], params[1], params[2]
	replica, _ := strconv.Atoi(replicaStr)

	//logger.Debug("handleReplicaBlock block %s from %s", blockhash, IPFromAddr(req.RemoteAddr))

	// get block from body
	data, err := readBody(bufio.NewReader(req.Body), req.ContentLength)
	if err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Failed read request body: %s", err.Error())
	}

	// verify data
	if len(data) == 0 {
		return SwarmErrorf(http.StatusBadRequest, "Empty data")
	}

	_, err = verifyBlock(blockhash, blocktype, &data)
	if err != nil {
		return SwarmErrorf(http.StatusBadRequest, "Mismatch replica blockhash %s, error: %s", blockhash, err.Error())
	}

	// do replica
	replicaChan := make(chan ReplicaBlockResult)

	nodes, err := DOMAIN_RING.GetReplicaNodes(blockhash, replica, true)
	if err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Isolated node for replica, error: %s", err.Error())
	}

	tasks := make(map[string]string)
	for nodeIp, deviceId := range nodes {
		tasks[nodeIp] = nodeIp
		blockInfo := NewBlockInfo(deviceId, blocktype, blockhash)
		go replicaBlock(replicaChan, nodeIp, blockInfo, &data)
	}

	// wait all finished
	failed := 0
Wait:
	for {
		select {
		case result := <-replicaChan:
			if result.status != STATUS_OK {
				// error
				logger.Error("Failed replica block %s, node %s, device %s", blockhash, result.nodeIp, result.deviceId)
				RecordAccessResult(result.nodeIp, ACTION_WRITE, false)
				failed += 1
			}
			RecordAccessResult(result.nodeIp, ACTION_WRITE, true)

			// finish a block
			delete(tasks, result.nodeIp)
			if len(tasks) == 0 {
				// all finished
				break Wait
			}
		}
	}

	if failed > 0 {
		success := replica - failed

		// retry other nodes with double replica first
		nodesRetry, err := DOMAIN_RING.GetReplicaNodes(blockhash, replica*2, true)
		if err != nil {
			// retry nodes with just failed 
			nodesRetry, err = DOMAIN_RING.GetReplicaNodes(blockhash, replica+failed, true)
			if err != nil {
				return SwarmErrorf(http.StatusInternalServerError, "Isolated node for replica, error: %s", err.Error())
			}
		}

		// skip tried nodes
		for nodeIp := range nodes {
			delete(nodesRetry, nodeIp)
		}

		// retry
		tasks = make(map[string]string)
		for nodeIp, deviceId := range nodesRetry {
			tasks[nodeIp] = nodeIp
			blockInfo := NewBlockInfo(deviceId, blocktype, blockhash)
			go replicaBlock(replicaChan, nodeIp, blockInfo, &data)
		}

	WaitRetry:
		for {
			select {
			case result := <-replicaChan:
				if result.status != STATUS_OK {
					// error
					logger.Error("Failed replica block %s, node %s, device %s", blockhash, result.nodeIp, result.deviceId)
					RecordAccessResult(result.nodeIp, ACTION_WRITE, false)
				}
				success += 1
				RecordAccessResult(result.nodeIp, ACTION_WRITE, true)

				// finish a block
				delete(tasks, result.nodeIp)
				if len(tasks) == 0 {
					// all finished
					break WaitRetry
				}
			}
		}

		if success < MIN_REPLICA {
			// not enough replica
			// since we must guarantee write reliability, return error here
			return SwarmErrorf(http.StatusInternalServerError, "Fatal in replica block %s", blockhash)
		}
	}

	// build response
	response := BaseResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	// finish
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}

	logger.Info("Done replica block %s.%s", blockhash, blocktype)
	return nil
}

// Distribute block to replica node and notify replicaChan the result
func replicaBlock(replicaChan chan ReplicaBlockResult, nodeIp string, blockInfo BlockInfo, pdata *[]byte) {
	defer onRecoverablePanic()

	result := ReplicaBlockResult{
		nodeIp:   nodeIp,
		deviceId: blockInfo.Device,
		status:   STATUS_OK,
	}

	// send block
	err := requestWriteBlock(nodeIp, blockInfo, pdata)
	if err != nil {
		logger.Error("Error in request replica block %s to node %s, error: %s", blockInfo, nodeIp, err.Error())
		result.status = STATUS_ERROR
		replicaChan <- result
		return
	}

	// notify
	replicaChan <- result
}

// Handle write block
func handleWriteBlock(writer http.ResponseWriter, req *http.Request) *SwarmError {
	MONITOR.SetVariable("M:BS:WriteBlock", 1)

	// check request
	if req.Method != "POST" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	if req.ContentLength < 1 {
		return SwarmErrorf(http.StatusBadRequest, "Invalid content length: %s", req.ContentLength)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		// to try best write block, no verify to device
		ParamVerifier{"deviceid", verifyNothing},
		ParamVerifier{"blocktype", verifyBlocktype},
		ParamVerifier{"blockhash", verifyBlockhash},
	})
	if swarmError != nil {
		return swarmError
	}
	deviceId, blocktype, blockhash := params[0], params[1], params[2]
	blockInfo := NewBlockInfo(deviceId, blocktype, blockhash)

	//logger.Debug("handleWriteBlock block %s from %s", blockhash, IPFromAddr(req.RemoteAddr))

	// get block from body
	data, err := readBody(bufio.NewReader(req.Body), req.ContentLength)
	if err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Failed read request body: %s", err.Error())
	}

	// verify data
	if len(data) == 0 {
		return SwarmErrorf(http.StatusBadRequest, "Empty data")
	}
	MONITOR.SetVariable("M:NET:Read", len(data))

	// write data
	err = writeBlockLocalDevice(blockInfo, &data)
	if err != nil {
		logger.Error("Failed write block %s, error: %s", blockInfo, err.Error())
		return SwarmErrorf(http.StatusInternalServerError, "Failed write block %s", blockInfo)
	}

	// build response
	response := BaseResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	// finish
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}

	logger.Debug("Done write block %s from node %s", blockInfo, IPFromAddr(req.RemoteAddr))
	return nil
}

// Handle read block
// First try read block from local deivces
// If proxied, then try to read block from hashring nodes
// Finally, try to read block on broadcast
func handleReadBlock(writer http.ResponseWriter, req *http.Request) *SwarmError {
	MONITOR.SetVariable("M:BS:ReadBlock", 1)

	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"deviceid", verifyDeviceId},
		ParamVerifier{"blocktype", verifyBlocktype},
		ParamVerifier{"blockhash", verifyBlockhash},
		ParamVerifier{"onlyheader", verifyNothing},
		ParamVerifier{"proxied", verifyNothing},
	})
	if swarmError != nil {
		return swarmError
	}
	deviceId, blocktype, blockhash, onlyheader, proxied := params[0], params[1], params[2], boolFromSting(params[3]), params[4]
	blockInfo := NewBlockInfo(deviceId, blocktype, blockhash)

	//logger.Debug("handleReadBlock block %s from %s", blockhash, IPFromAddr(req.RemoteAddr))

	// try read block from local on specified device
	pdata, err := readBlockLocalDevice(blockInfo, onlyheader)
	if err == nil {
		// finish
		writer.Write(*pdata)
		MONITOR.SetVariable("M:NET:Write", len(*pdata))
		return nil
	}

	// used to search block location
	blockAnyDevice := NewBlockAnyDevice(blockInfo.Type, blockInfo.Hash)

	// try read block from local on any devices
	deviceId, err = findBlockDevice(blockAnyDevice)
	if err == nil {
		// change to target device
		blockInfoTarget := NewBlockInfoFromAnyDevice(deviceId, blockAnyDevice)
		// try read block from local
		pdata, err := readBlockLocalDevice(blockInfoTarget, onlyheader)
		if err == nil {
			// finish
			writer.Write(*pdata)
			MONITOR.SetVariable("M:NET:Write", len(*pdata))
			return nil
		}
	}

	if proxied != STATUS_PROXIED {
		// TODO: just proxy when err is no such block file.
		return SwarmErrorf(http.StatusNotFound, "Not exist block %s read by %s", blockInfo, IPFromAddr(req.RemoteAddr))
	}

	// try read block on hashring
	nodeIp, deviceId, err := locateBlockByRing(blockAnyDevice)
	if err == nil {
		blockInfoTarget := NewBlockInfoFromAnyDevice(deviceId, blockAnyDevice)
		pdata, err := readBlockRemoteDevice(nodeIp, blockInfoTarget, onlyheader)
		if err == nil {
			// finish
			writer.Write(*pdata)
			MONITOR.SetVariable("M:NET:Write", len(*pdata))
			return nil
		}
	}

	// the last resort, try read block on broadcast, and very expensive
	// TODO: restrain this request to avoid degrade entire domain
	nodes, err := broadcastBlockNode(blockAnyDevice, true)
	if err != nil {
		return SwarmErrorf(http.StatusNotFound, "Failed read block %s by broadcast", blockAnyDevice)
	}

	for nodeIp, deviceId := range nodes {
		blockInfoTarget := NewBlockInfoFromAnyDevice(deviceId, blockAnyDevice)
		pdata, err := readBlockRemoteDevice(nodeIp, blockInfoTarget, onlyheader)
		if err == nil {
			// finish
			writer.Write(*pdata)
			MONITOR.SetVariable("M:NET:Write", len(*pdata))
			return nil
		}
	}

	// should not be here
	return SwarmErrorf(http.StatusBadRequest, "Fatal read block %s", blockInfo)
}

// Locate block in local devices
func findBlockDevice(blockAnyDevice BlockInfo) (deviceId string, err error) {
	// try locate in meta cache
	deviceId = META_CACHE.FindBlockDevice(blockAnyDevice)
	if deviceId != "" {
		// found in meta cache
		return deviceId, nil
	}

	// try devices directly
	for deviceId, _ = range LOCAL_DEVICES.Data {
		blockInfoTarget := NewBlockInfo(deviceId, blockAnyDevice.Type, blockAnyDevice.Hash)
		blockPath, err := getBlockPath(blockInfoTarget)
		if err != nil {
			return "", err
		}

		if ExistPath(blockPath) {
			return deviceId, nil
		}
	}
	err = fmt.Errorf("Block %s not found in this node.", blockAnyDevice)
	return
}

// Read block from local specified device
func readBlockLocalDevice(blockInfo BlockInfo, onlyheader bool) (pdata *[]byte, err error) {
	blockPath, err := getBlockPath(blockInfo)
	if err != nil {
		return
	}
	if !ExistPath(blockPath) {
		err = fmt.Errorf("Block %s not found in this node.", blockInfo)
		return
	}

	// read block from local
	pdata, err = DEVICE_MANAGER.ReadFile(blockInfo, 0, 0)
	if err != nil {
		logger.Error("Failed read block %s from local device, error: %s", blockInfo, err.Error())
		return
	}

	// verify
	_, err = verifyBlock(blockInfo.Hash, blockInfo.Type, pdata)
	if err != nil {
		ALARM_MANAGER.Add(ALARM_MISMATCH_BLOCKHASH, err, "Mismatch hash, block %s", blockInfo)
		return
	}

	if onlyheader {
		r := (*pdata)[0:HEADER_SIZE]
		return &r, nil
	}

	return pdata, nil
}

// Read block from remote node with specified device
func readBlockRemoteDevice(nodeIp string, blockInfo BlockInfo, onlyheader bool) (pdata *[]byte, err error) {
	gatherChan := make(chan ReadBlockResult)
	go gatherBlock(gatherChan, nodeIp, blockInfo, onlyheader, STATUS_NOT_PROXIED)
	// blocking wait
	select {
	case result := <-gatherChan:
		if result.status != STATUS_OK {
			RecordAccessResult(nodeIp, ACTION_READ, false)
			return nil, fmt.Errorf("Failed read block %s from %s", blockInfo, nodeIp)
		}
		// read success
		RecordAccessResult(nodeIp, ACTION_READ, true)
		_, err = verifyBlock(blockInfo.Hash, blockInfo.Type, result.pdata)
		if err != nil {
			return nil, err
		}
		return result.pdata, nil
	}
	// should not be here
	return nil, fmt.Errorf("Fatal read block %s from %s", blockInfo, nodeIp)
}

type BlockStatusResult struct {
	nodeIp          string
	deviceId        string
	status          int
	headerType      int8
	headerState     int8
	headerTimestamp time.Time
	headerSize      int64
}

// Query block in hashring for read, not try local devices
// Since this requires network traffic, therefor is a little expensive.
func locateBlockByRing(blockAnyDevice BlockInfo) (nodeIp string, deviceId string, err error) {
	// get nodes on hashring
	nodes, err := DOMAIN_RING.GetReplicaNodes(blockAnyDevice.Hash, REPLICA, true)
	if err != nil {
		return
	}

	// detect node
	taskNum := REPLICA
	blockStatusChan := make(chan BlockStatusResult)
	for nodeIp, deviceId := range nodes {
		if nodeIp == NODE_ADDRESS {
			// skip local
			taskNum -= 1
			continue
		}
		blockInfoTarget := NewBlockInfo(deviceId, blockAnyDevice.Type, blockAnyDevice.Hash)
		go queryBlockStatus(blockStatusChan, nodeIp, blockInfoTarget)
	}

	// wait all detect finished or found one
	for {
		select {
		case result := <-blockStatusChan:
			if result.status == STATUS_OK {
				// found
				nodeIp = result.nodeIp
				deviceId = result.deviceId
				return nodeIp, deviceId, nil
			}
			taskNum -= 1
			if taskNum == 0 {
				// all finished
				err = fmt.Errorf("Detect none node of block %s", blockAnyDevice)
				return
			}
		}
	}

	// should not be here
	err = fmt.Errorf("Fatal locate block %s in domain", blockAnyDevice)
	return
}

// Broadcast detect block node
func broadcastBlockNode(blockAnyDevice BlockInfo, justone bool) (nodes map[string]string, err error) {
	// init
	nodes = make(map[string]string)

	// request broadcast
	taskId := Uuid()

	task := Task{
		TaskId:   taskId,
		Expire:   time.Now().Add(time.Duration(time.Second * 1)), // time.Millisecond*10
		ReqChan:  make(chan string, 1),
		RespChan: make(chan string, 1),
		AckChan:  make(chan string, 1),
	}

	//logger.Debug("requester send REGISTER_CHAN")
	REGISTER_CHAN <- task

	// send req
	request := BroadcastRequest{
		TaskId:         taskId,
		BlockAnyDevice: blockAnyDevice,
	}

	reqJson, err := json.Marshal(request)
	if err != nil {
		err = fmt.Errorf("Encode error: %s", err.Error())
		return
	}

	task.ReqChan <- string(reqJson)

	// wait resp
	for {
		select {
		case resp := <-task.RespChan:
			// send ack
			task.AckChan <- ""

			// parse response
			var response BroadcastResponse
			err = json.Unmarshal([]byte(resp), &response)
			if err != nil {
				continue
			}

			if response.Status != STATUS_OK {
				continue
			}

			// found
			nodes[response.NodeIp] = response.DeviceId
			if justone {
				// one is enough
				return nodes, nil
			}

		case <-time.After(DETECT_BLOCK_TIMEOUT):
			if len(nodes) == 0 {
				// timeout with no result
				err = fmt.Errorf("Timeout detect block %s", blockAnyDevice)
				MONITOR.SetVariable("M:BR:Timeout", 1)
				return
			}
			return nodes, nil
		}
	}
	return nodes, nil
}

// Verify block
func verifyBlock(blockhash string, blocktype string, pdata *[]byte) (blockHeader BlockHeader, err error) {
	if pdata == nil || len(*pdata) == 0 {
		err = fmt.Errorf("Empty block")
		return
	}

	// load header
	blockHeader, err = LoadBlockHeader((*pdata)[0:HEADER_SIZE])
	if err != nil {
		err = fmt.Errorf("Failed to parse blockheader: %s.", blockhash)
		return
	}

	// verify hash
	if blockhash != Sha1Hex((*pdata)[HEADER_SIZE:]) {
		err = fmt.Errorf("Mismatch hash of block %s.%s.", blockhash, blocktype)
		return
	}

	return blockHeader, nil
}

// Save block to local block file
// Try best to write blocks. 
// If target device is offline or removed, handoff to other devices.
// Handed off blocks will be rebalanced by repair and gc process.
func writeBlockLocalDevice(blockInfo BlockInfo, pdata *[]byte) (err error) {
	// verify
	_, err = verifyBlock(blockInfo.Hash, blockInfo.Type, pdata)
	if err != nil {
		return fmt.Errorf("Mismatch write block %s", blockInfo)
	}

	// just try write online device
	if LOCAL_DEVICES.IsLocalOnlineDevice(blockInfo.Device) {
		if err = tryWriteBlock(blockInfo, pdata); err == nil {
			// success write to origin target device
			return
		}
	}

	// handoff
	tried := NewStringSet()
	tried.Add(blockInfo.Device)

	for priority := 1; err != nil; priority++ {
		deviceId, e := choiceHandoffDevice(blockInfo, priority, tried)
		if e != nil {
			// out of devices
			return fmt.Errorf("Failed handoff block %s", blockInfo)
		}

		// save tried
		tried.Add(deviceId)

		// try handoff
		blockInfoHandoff := NewBlockInfo(deviceId, blockInfo.Type, blockInfo.Hash)
		err = tryWriteBlock(blockInfoHandoff, pdata)
	}

	// handoff success 
	return
}

// Get handoff device.
// If choose device by ring, handoff device will focus on a particular one, 
// which may conduce double load to that device.
// We choose a random online device here.
func choiceHandoffDevice(blockInfo BlockInfo, priority int, tried *StringSet) (deviceId string, err error) {
	all := make([]string, 0)
	for deviceId := range LOCAL_DEVICES.LocalOnlineDevices().Data {
		if tried.Contains(deviceId) {
			// skip tried
			continue
		}
		all = append(all, deviceId)
	}

	deviceId, err = ChoiceString(all)
	return
}

// Try write block to target
func tryWriteBlock(blockInfo BlockInfo, pdata *[]byte) (err error) {
	// get blockpath
	blockPath, err := getBlockPath(blockInfo)
	if err != nil {
		return err
	}

	// write block
	err = DEVICE_MANAGER.WriteFile(blockInfo, 0, pdata)
	if err != nil {
		logger.Error("Failed write block file %s, error: %s", blockPath, err.Error())
		return
	}
	logger.Debug("Write block file %s", blockPath)

	// save block meta
	META_CACHE.Save(blockInfo)

	return nil
}

// Handle query block status with device specified
type BlockStatusResponse struct {
	Version         string
	Timestamp       time.Time
	Status          int
	HeaderType      int8
	HeaderState     int8
	HeaderTimestamp time.Time
	HeaderSize      int64
}

func handleBlockStatus(writer http.ResponseWriter, req *http.Request) *SwarmError {
	MONITOR.SetVariable("M:BS:BlockStatus", 1)

	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"deviceid", verifyDeviceId},
		ParamVerifier{"blockhash", verifyBlockhash},
		ParamVerifier{"blocktype", verifyBlocktype},
	})
	if swarmError != nil {
		return swarmError
	}
	deviceId, blockhash, blocktype := params[0], params[1], params[2]
	blockInfo := NewBlockInfo(deviceId, blocktype, blockhash)

	//logger.Debug("handleBlockStatus block %s from %s", blockhash, IPFromAddr(req.RemoteAddr))

	// build response
	response := BlockStatusResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	// check block exists
	blockHeader, err := getHeaderFromBlockFile(blockInfo)
	if err != nil {
		response.Status = STATUS_ERROR
	} else {
		// success
		response.HeaderType = blockHeader.Type
		response.HeaderState = blockHeader.State
		response.HeaderTimestamp = blockHeader.Timestamp
		response.HeaderSize = blockHeader.Size
	}

	// finish
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}

	logger.Info("Query block %s with status %d", blockInfo, response.Status)
	return nil
}

// Handle query block exist with device specified
type BlockExistResponse struct {
	Version   string
	Timestamp time.Time
	Status    int
}

func handleBlockExist(writer http.ResponseWriter, req *http.Request) *SwarmError {
	MONITOR.SetVariable("M:BS:BlockExist", 1)

	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"deviceid", verifyDeviceId},
		ParamVerifier{"blockhash", verifyBlockhash},
		ParamVerifier{"blocktype", verifyBlocktype},
	})
	if swarmError != nil {
		return swarmError
	}
	deviceId, blockhash, blocktype := params[0], params[1], params[2]
	blockInfo := NewBlockInfo(deviceId, blocktype, blockhash)

	// build response
	response := BaseResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	// check block exists
	if !META_CACHE.BlockInMetacache(blockInfo) {
		response.Status = STATUS_ERROR
	}

	// finish
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}

	return nil
}

// Handle del block
// Make object block to del state
// blockhash is objecthash
// If not proxied, del locally
// Or del from hashring
func handleDelBlock(writer http.ResponseWriter, req *http.Request) *SwarmError {
	MONITOR.SetVariable("M:BS:DelBlock", 1)

	// check request
	if req.Method != "POST" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"deviceid", verifyDeviceId},
		ParamVerifier{"blocktype", verifyBlocktype},
		ParamVerifier{"blockhash", verifyBlockhash},
	})
	if swarmError != nil {
		return swarmError
	}
	deviceId, blocktype, blockhash := params[0], params[1], params[2]
	blockInfo := NewBlockInfo(deviceId, blocktype, blockhash)

	//logger.Debug("handleDelBlock block %s from %s", blockhash, IPFromAddr(req.RemoteAddr))

	// del
	err := delBlockLocalDevice(blockInfo)
	if err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Failed del block %s, error: %s", blockInfo, err.Error())
	}

	// build response
	response := BaseResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	// finish
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}

	return nil
}

// Del block from local device
func delBlockLocalDevice(blockInfo BlockInfo) (err error) {
	// prepare local path
	blockPath, err := getBlockPath(blockInfo)
	if err != nil {
		return
	}
	if ExistPath(blockPath) {
		err = os.Remove(blockPath)
		if err != nil {
			logger.Error("Failed to remove rebalance gc block %s", blockInfo)
			return
		}
	}
	return
}

// This just modify header state to DEL
func markBlockDelLocal(blockInfo BlockInfo) (err error) {
	// prepare local path
	blockPath, err := getBlockPath(blockInfo)
	if err != nil {
		return
	}
	if !ExistPath(blockPath) {
		return fmt.Errorf("File %s not exists", blockPath)
	}

	// get block header
	blockHeader, err := getHeaderFromBlockFile(blockInfo)
	if err != nil {
		return
	}

	// modify header
	blockHeader.State = HEADER_STATE_DEL
	blockHeader.Timestamp = time.Now()

	// dump 
	headerBytes, err := DumpBlockHeader(blockHeader)
	if err != nil {
		return
	}

	// write back
	// Write returns a non-nil error when n != len(b)
	f, err := os.OpenFile(blockPath, os.O_RDWR, 0666)
	if err != nil {
		return
	}
	_, err = f.WriteAt(headerBytes, 0)
	if err != nil {
		return
	}

	logger.Debug("Deleted local block file %s", blockPath)
	return nil
}

// Request write block
func requestWriteBlock(nodeIp string, blockInfo BlockInfo, pdata *[]byte) *SwarmError {
	logger.Debug("Request write block %s to node %s ", blockInfo, nodeIp)

	query := url.Values{
		"deviceid":  []string{blockInfo.Device},
		"blocktype": []string{blockInfo.Type},
		"blockhash": []string{blockInfo.Hash},
	}

	_, err := RequestHttp(nodeIp, BLOCK_PORT, URL_SWARM_WRITE_BLOCK, query, "POST", pdata, true, WRITE_BLOCK_TIMEOUT, true)
	if err != nil {
		logger.Error("Failed request write block %s to node %s, error: %s", blockInfo, nodeIp, err.Error())
		return SwarmErrorf(http.StatusInternalServerError, err.Error())
	}
	MONITOR.SetVariable("M:NET:Write", len(*pdata))

	return nil
}

// Request read block
func requestReadBlock(nodeIp string, blockInfo BlockInfo, onlyheader bool, proxied string) (pdata *[]byte, err error) {
	logger.Debug("Request read block %s from node %s", blockInfo, nodeIp)

	query := url.Values{
		"deviceid":   []string{blockInfo.Device},
		"blocktype":  []string{blockInfo.Type},
		"blockhash":  []string{blockInfo.Hash},
		"onlyheader": []string{boolToString(onlyheader)},
		"proxied":    []string{proxied},
	}

	pdata, err = RequestHttp(nodeIp, BLOCK_PORT, URL_SWARM_READ_BLOCK, query, "GET", nil, false, READ_BLOCK_TIMEOUT, true)
	if err != nil {
		return nil, err
	}
	MONITOR.SetVariable("M:NET:Read", len(*pdata))

	return
}

// Request replica block
func requestReplicaBlock(nodeIp string, blocktype string, blockhash string, pdata *[]byte, replica int) (err error) {
	logger.Debug("Request replica %d block %s.%s to %s", replica, blockhash, blocktype, nodeIp)

	query := url.Values{
		"blocktype": []string{blocktype},
		"blockhash": []string{blockhash},
		"replica":   []string{strconv.Itoa(replica)},
	}

	_, err = RequestHttp(nodeIp, BLOCK_PORT, URL_SWARM_REPLICA_BLOCK, query, "POST", pdata, true, REPLICA_BLOCK_TIMEOUT, true)
	if err != nil {
		logger.Error("Failed request replica block %s node: %s, error: %s", blockhash, nodeIp, err.Error())
		return
	}
	MONITOR.SetVariable("M:NET:Write", len(*pdata))

	return nil
}

// Check block exist on remote node
func requestBlockStatus(nodeIp string, blockInfo BlockInfo) (response BlockStatusResponse, err error) {
	logger.Debug("Request status for block %s on node %s", blockInfo, nodeIp)

	query := url.Values{
		"deviceid":  []string{blockInfo.Device},
		"blockhash": []string{blockInfo.Hash},
		"blocktype": []string{blockInfo.Type},
	}

	// do not parseResponse since response is not BaseResponse
	pdata, err := RequestHttp(nodeIp, BLOCK_PORT, URL_SWARM_BLOCK_STATUS, query, "GET", nil, false, QUERY_BLOCK_TIMEOUT, true)
	if err != nil {
		logger.Error("Failed request query block %s node %s, error: %s", blockInfo, nodeIp, err.Error())
		return
	}

	err = json.Unmarshal(*pdata, &response)
	if err != nil {
		return
	}

	return response, nil
}

// Query block status with device specified
func queryBlockStatus(blockStatusChan chan BlockStatusResult, nodeIp string, blockInfo BlockInfo) {
	defer onRecoverablePanic()

	result := BlockStatusResult{
		nodeIp:   nodeIp,
		deviceId: blockInfo.Device,
		status:   STATUS_OK,
	}

	// send block
	response, err := requestBlockStatus(nodeIp, blockInfo)
	if err != nil {
		logger.Debug("Failed query status block %s node %s, error: %s", blockInfo, nodeIp, err.Error())
		result.status = STATUS_ERROR
		blockStatusChan <- result
		return
	}

	result.headerType = response.HeaderType
	result.headerState = response.HeaderState
	result.headerTimestamp = response.HeaderTimestamp
	result.headerSize = response.HeaderSize

	// notify
	blockStatusChan <- result
}

// Check block exist on remote node
func requestBlockExist(nodeIp string, blockInfo BlockInfo) (response BlockExistResponse, err error) {
	logger.Debug("Request Exist for block %s on node %s", blockInfo, nodeIp)

	query := url.Values{
		"deviceid":  []string{blockInfo.Device},
		"blockhash": []string{blockInfo.Hash},
		"blocktype": []string{blockInfo.Type},
	}

	// do not parseResponse since response is not BaseResponse
	pdata, err := RequestHttp(nodeIp, BLOCK_PORT, URL_SWARM_BLOCK_EXIST, query, "GET", nil, false, QUERY_BLOCK_TIMEOUT, true)
	if err != nil {
		logger.Error("Failed request exist block %s node %s, error: %s", blockInfo, nodeIp, err.Error())
		return
	}

	err = json.Unmarshal(*pdata, &response)
	if err != nil {
		return
	}

	return response, nil
}

// Request del block
func requestDelBlock(nodeIp string, blockInfo BlockInfo) (err error) {
	logger.Debug("Request del block %s from node %s", blockInfo, nodeIp)

	query := url.Values{
		"deviceid":  []string{blockInfo.Device},
		"blocktype": []string{blockInfo.Type},
		"blockhash": []string{blockInfo.Hash},
	}

	_, err = RequestHttp(nodeIp, BLOCK_PORT, URL_SWARM_DEL_BLOCK, query, "POST", nil, true, WRITE_BLOCK_TIMEOUT, true)
	if err != nil {
		logger.Error("Failed del block %s from node %s, error: %s", blockInfo, nodeIp, err.Error())
		return
	}

	return nil
}
