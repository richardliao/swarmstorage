package swarm

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type WriteBlockResult struct {
	blockhash string
	status    int
}

type ReadBlockResult struct {
	pdata     *[]byte
	blockhash string
	status    int
}

type DelBlockResult struct {
	blockhash string
	blocktype string
	status    int
}

type QueueReadBlockResult struct {
	pdata     *[]byte
	blockhash string
	index     int
	status    int
}

//------------------------------------------------------------------------------
// OSS: Object Storage Server
//------------------------------------------------------------------------------

type OSS struct {
	WorkerBase
	watchdog *Watchdog
}

// Implement interface Restartable
func (this *OSS) Name() string {
	return "OSS"
}

// Implement interface Restartable
func (this *OSS) Start(r Restartable, e interface{}) {
	defer func() {
		e := recover()
		this.OnPanic(this, this.watchdog, e)
	}()

	// do stuff
	logger.Critical("OSS started.")

	http.Handle(URL_SWARM_WRITE_OSS, httpHandler(handleWriteObject))
	http.Handle(URL_SWARM_READ_OSS, httpHandler(handleReadObject))
	http.Handle(URL_SWARM_QUERY_OSS, httpHandler(handleQueryObject))

	err := http.ListenAndServe(OSS_ADDRESS+":"+OSS_PORT, nil)
	checkFatal(err)

	logger.Critical("OSS stopped.")
}

// Handle write object
type ObjectWriteResponse struct {
	Version    string
	Timestamp  time.Time
	Status     int
	Objecthash string
	Size       int64
	Signature  string
}

/*
Methods to write object:
 1. blocked object, usually object smaller than 2 blocks, with objecthash

 2. indexed object, usally large object
  * with objecthash
  * streaming without know size and objecthash in advance
  * based on other indexed object with offset w/o objecthash

Writing indexed object will split object into blocks, and distribute blocks to 
block server for replica. Afte replicating finished, calc and distribute objecthash.
If objecthash mismatch, client will get an error response. 
Since block is self consistent, there is no risk to write wrong block content. 
Orphan blocks not in any indexed object can be fixed by successfully retry upload 
the indexed object.
*/
func handleWriteObject(writer http.ResponseWriter, req *http.Request) *SwarmError {
	MONITOR.SetVariable("M:FS:WriteObject", 1)

	// check request
	if req.Method != "POST" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"blocktype", verifyObjectBlocktype},
	})
	if swarmError != nil {
		return swarmError
	}
	blocktype := params[0]

	// optional params
	objecthash, _ := getParam(req, "objecthash")
	baseobject, _ := getParam(req, "baseobject")
	offsetStr, _ := getParam(req, "offset")
	offset, _ := strconv.ParseInt(offsetStr, 10, 64)

	//logger.Debug("handleWriteObject objecthash %s from %s", objecthash, IPFromAddr(req.RemoteAddr))

	if blocktype == BLOCK_TYPE_OBJECT {
		if len(objecthash) == 0 {
			// write object require objecthash
			return SwarmErrorf(http.StatusBadRequest, "Empty objecthash for write object")
		}
	}

	// vars and init
	var totalSize int64
	var blockIndex int64
	var caclObjecthash string
	c := make(chan WriteBlockResult)
	timestamp := time.Now()

	if blocktype == BLOCK_TYPE_INDEX {
		// get object content from body
		reader := bufio.NewReaderSize(req.Body, BLOCK_SIZE)

		// TODO: Splitter interface
		blockhashes := make([]string, 0)
		tasks := make(map[string]string)

		if len(baseobject) != 0 {
			// write based on baseobject

			// set totalSize to offset
			totalSize = offset

			// get base joinhashes
			joinhashes, _, offsetInBlock, err := getOffsetBlockhashes(offset, baseobject)
			if err != nil {
				return SwarmErrorf(http.StatusNotFound, err.Error())
			}

			// split joinhashes
			blockIndex = offset / int64(BLOCK_SIZE)
			wholeBlockhashes := group(joinhashes, HASH_HEX_SIZE)

			// set exist block hashes
			blockhashes = wholeBlockhashes[:blockIndex]

			// get first offset block
			offsetBlockhash := wholeBlockhashes[blockIndex]
			pdata, err := readBlockInDomain(offsetBlockhash, BLOCK_TYPE_BLOCK, false, STATUS_PROXIED)
			if err != nil {
				return SwarmErrorf(http.StatusNotFound, err.Error())
			}

			// receive remain first block content
			remainOffsetSize := int64(BLOCK_SIZE) - offsetInBlock
			if remainOffsetSize > 0 {
				remainBlock, ioerr := readBody(reader, remainOffsetSize)
				if ioerr != nil && ioerr != io.EOF && ioerr != io.ErrUnexpectedEOF {
					return SwarmErrorf(http.StatusInternalServerError, "Failed read request body: %s", ioerr.Error())
				}

				// distribute block
				MONITOR.SetVariable("M:NET:Read", len(remainBlock))
				totalSize += int64(len(remainBlock))

				// completed block
				block := append((*pdata)[HEADER_SIZE:HEADER_SIZE+offsetInBlock], remainBlock...)

				// calc blockhash
				blockhash := Sha1Hex(block)
				blockhashes = append(blockhashes, blockhash)

				// distribute block
				tasks[blockhash] = blockhash
				pdata, err := buildBlock(blockhash, blockhash, timestamp, 0, &block)
				if err != nil {
					return SwarmErrorf(http.StatusInternalServerError, "Failed build block content: %s", err.Error())
				}

				go distributeBlock(c, blockhash, BLOCK_TYPE_BLOCK, pdata)
			}
		}

		for blockIndex++; ; blockIndex++ {
			block, ioerr := readBody(reader, int64(BLOCK_SIZE))
			if ioerr != nil && ioerr != io.EOF && ioerr != io.ErrUnexpectedEOF {
				return SwarmErrorf(http.StatusInternalServerError, "Failed read request body: %s", ioerr.Error())
			}

			if len(block) > 0 {
				MONITOR.SetVariable("M:NET:Read", len(block))
				totalSize += int64(len(block))

				// calc blockhash
				blockhash := Sha1Hex(block)
				blockhashes = append(blockhashes, blockhash)

				// distribute block
				if _, ok := tasks[blockhash]; !ok {
					tasks[blockhash] = blockhash
					pdata, err := buildBlock(blockhash, blockhash, timestamp, 0, &block)
					if err != nil {
						return SwarmErrorf(http.StatusInternalServerError, "Failed build block content: %s", err.Error())
					}

					go distributeBlock(c, blockhash, BLOCK_TYPE_BLOCK, pdata)
				}
			}

			if ioerr == io.EOF || ioerr == io.ErrUnexpectedEOF {
				// end read
				break
			}
		}

		// verify objecthash if provided
		joinhashes := strings.Join(blockhashes, "")
		caclObjecthash = Sha1Hex([]byte(joinhashes))
		if len(objecthash) != 0 && objecthash != caclObjecthash {
			return SwarmErrorf(http.StatusInternalServerError, "Mismatch objecthash: %s", objecthash)
		}

		// wait all blocks finished
	Wait:
		for {
			select {
			case result := <-c:
				if result.status != STATUS_OK {
					// TODO: error
					return SwarmErrorf(http.StatusInternalServerError, "Fatal in distribute block: %s", result.blockhash)
				}
				// finish a block
				delete(tasks, result.blockhash)
				if len(tasks) == 0 {
					// all finished
					break Wait
				}
			}
		}

		// distribute index block
		block := []byte(joinhashes)
		pdata, err := buildBlock(caclObjecthash, BLOCK_TYPE_INDEX, timestamp, totalSize, &block)
		if err != nil {
			return SwarmErrorf(http.StatusInternalServerError, "Failed build block content: %s", err.Error())
		}
		go distributeBlock(c, caclObjecthash, BLOCK_TYPE_INDEX, pdata)
		result := <-c
		if result.status != STATUS_OK {
			return SwarmErrorf(http.StatusInternalServerError, "Fatal in distribute blockhashes: %s", result.blockhash)
		}
	} else {
		// single block object
		block, err := ioutil.ReadAll(req.Body)
		if err != nil {
			return SwarmErrorf(http.StatusInternalServerError, "Failed read request body: %s", err.Error())
		}
		totalSize = int64(len(block))
		MONITOR.SetVariable("M:NET:Read", totalSize)

		// verify objecthash
		caclObjecthash = Sha1Hex(block)
		if objecthash != caclObjecthash {
			// write object always require match objecthash
			return SwarmErrorf(http.StatusBadRequest, "Mismatch objecthash: %s", objecthash)
		}

		// distribute object block
		pdata, err := buildBlock(objecthash, BLOCK_TYPE_OBJECT, timestamp, totalSize, &block)
		if err != nil {
			return SwarmErrorf(http.StatusInternalServerError, "Failed build block content: %s", err.Error())
		}
		go distributeBlock(c, objecthash, BLOCK_TYPE_OBJECT, pdata)
		result := <-c
		if result.status != STATUS_OK {
			return SwarmErrorf(http.StatusInternalServerError, "Fatal in distribute blockhashes: %s", result.blockhash)
		}
	}

	// build response
	response := ObjectWriteResponse{
		Version:    SWARM_VERSION,
		Timestamp:  time.Now(),
		Status:     STATUS_OK,
		Objecthash: caclObjecthash,
		Size:       totalSize,
	}
	genObjectWriteSignature(&response)

	// finish
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}

	logger.Info("Wrote object %s.%s", caclObjecthash, blocktype)
	return nil
}

// TODO: Gen signature for ObjectWriteResponse
func genObjectWriteSignature(response *ObjectWriteResponse) {
	response.Signature = ""
}

// Build content
// Prepend header to block bytes
func buildBlock(blockhash string, blocktype string, timestamp time.Time, size int64, pblock *[]byte) (blockContent *[]byte, err error) {
	// build block header
	var blockHeader BlockHeader
	blockHeader, err = NewBlockHeader(blocktype, HEADER_STATE_NEW, timestamp, size, blockhash)
	if err != nil {
		return
	}
	// dump
	var headerBytes []byte
	headerBytes, err = DumpBlockHeader(blockHeader)
	if err != nil {
		return
	}

	// write to buf
	// TODO: zero copy
	buf := bytes.NewBuffer(headerBytes)
	_, err = buf.Write(*pblock)
	if err != nil {
		return
	}

	data := buf.Bytes()
	return &data, nil
}

// Distribute block to node for replica
// Try nodes on hashring by sequence, stop if one success.
// If all failed, write object will be failed.
func distributeBlock(c chan WriteBlockResult, blockhash string, blocktype string, pdata *[]byte) {
	defer onRecoverablePanic()

	result := WriteBlockResult{
		blockhash: blockhash,
		status:    STATUS_ERROR,
	}

	// get target node and device
	// try REPLICA nodes
	nodes, err := DOMAIN_RING.GetReplicaNodes(blockhash, REPLICA, true)
	if err != nil {
		logger.Error("Isolated node for proxy read, error: %s", err.Error())
		result.status = STATUS_ERROR
		c <- result
		return
	}
	for nodeIp, deviceId := range nodes {
		// send block
		err := requestReplicaBlock(nodeIp, blocktype, blockhash, pdata, REPLICA)
		if err != nil {
			logger.Error("Error in request write block %s to node %s device %s, error: %s", blockhash, nodeIp, deviceId, err.Error())
			RecordAccessResult(nodeIp, ACTION_WRITE, false)
			// try next node
			continue
		}
		// success write
		result.status = STATUS_OK
		RecordAccessResult(nodeIp, ACTION_WRITE, true)
		// no more try
		break
	}

	// notify
	c <- result
}

// Read block at offset
func getOffsetBlockhashes(offset int64, objecthash string) (joinhashes string, objectsize int64, offsetInBlock int64, err error) {
	// read whole blockhashes
	pdata, err := readBlockInDomain(objecthash, BLOCK_TYPE_INDEX, false, STATUS_PROXIED)
	if err != nil {
		return
	}
	objectsize, joinhashes, _, err = parseBlockhashes(objecthash, pdata)
	if err != nil {
		return
	}

	if offset > objectsize {
		err = fmt.Errorf("Offset %d exceed object size %d of object %s", offset, objectsize, objecthash)
		return
	}

	//// set del object to not found
	//if blockHeader.State == HEADER_STATE_DEL {
	//	err = fmt.Errorf("Object %s not exists.", objecthash)
	//	return
	//}

	// calc first block by offset
	offsetInBlock = offset % int64(BLOCK_SIZE)
	return
}

// Handle read object
// First read objecthash block, and parse blockhashes for blocks to read. Then 
// read blocks sequencely.
// A buffer is used to prefetch blocks. The size of buffer depends on the speed
// of client consuming blocks.
// We put all params in url instead of RESTful, and not using HTTP Range header,
// since the transport protocol may be changed in the future for any reason.
func handleReadObject(writer http.ResponseWriter, req *http.Request) *SwarmError {
	MONITOR.SetVariable("M:FS:ReadObject", 1)

	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"objecthash", verifyObjecthash},
		ParamVerifier{"blocktype", verifyObjectBlocktype},
	})
	if swarmError != nil {
		return swarmError
	}
	objecthash, blocktype := params[0], params[1]

	//logger.Debug("handleReadObject objecthash %s from %s", objecthash, IPFromAddr(req.RemoteAddr))

	// optional param
	offset := int64(0)
	offsetStr, err := getParam(req, "offset")
	if err == nil {
		offset, err = strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			return SwarmErrorf(http.StatusBadRequest, err.Error())
		}
	}
	logger.Debug("Reading object %s offset %d", objecthash, offset)

	if blocktype == BLOCK_TYPE_INDEX {
		joinhashes, objectsize, offsetInBlock, err := getOffsetBlockhashes(offset, objecthash)
		if err != nil {
			return SwarmErrorf(http.StatusNotFound, err.Error())
		}

		blockIndex := offset / int64(BLOCK_SIZE)
		blockhashes := group(joinhashes, HASH_HEX_SIZE)[blockIndex:]

		// set header Content-Length
		writer.Header().Set("Content-Length", Itoa(objectsize-offset))

		// read blocks
		numBlock := len(blockhashes)
		blockChan := make(chan QueueReadBlockResult)
		blockBuffer := make(map[int]*[]byte)
		indexWrite := 0
		queueLength := PREFETCH_QUEUQ_LENGTH
		maxQueue := PREFETCH_QUEUQ_LENGTH

		// start dispatch
		go dispatchReadBlock(blockChan, &queueLength, blockhashes)

		for {
			select {
			case result := <-blockChan:
				if result.status != STATUS_OK {
					http.Error(writer, "", http.StatusInternalServerError)
					return nil
				}

				// put to buffer
				blockBuffer[result.index] = result.pdata

			default:
				// get block
				pdata, ok := blockBuffer[indexWrite]
				if !ok {
					// not ready
					time.Sleep(1e7)
					continue
				}

				// send response
				// TODO: write chunk stream
				writer.Write((*pdata)[HEADER_SIZE+offsetInBlock:])

				// reset offsetInBlock after write first block
				offsetInBlock = 0

				// delete from buffer
				delete(blockBuffer, indexWrite)

				// adjust queue
				indexWrite += 1
				if queueLength < maxQueue {
					atomic.AddInt32(&queueLength, 1)
					if queueLength < 0 {
						// should never be here
						queueLength = 0
					}
				}

				if indexWrite >= numBlock {
					// finish all blocks
					logger.Info("Finish read object %s", objecthash)
					return nil
				}

				// incr queue window
				if indexWrite != 0 && maxQueue < 10 && len(blockBuffer) == 0 {
					// write block takes a while, so slide not too frequently
					// buffer is empty, so incr max queue
					maxQueue += 1
				}
			}
		}
	} else {
		// object is single block
		pdata, err := readBlockInDomain(objecthash, BLOCK_TYPE_OBJECT, false, STATUS_PROXIED)
		if err != nil {
			return SwarmErrorf(http.StatusNotFound, err.Error())
		}

		// parse header
		blockHeader, err := LoadBlockHeader((*pdata)[0:HEADER_SIZE])
		if err != nil {
			logger.Error("Failed load blockHeader %x, error: %s", (*pdata), err.Error())
			return SwarmErrorf(http.StatusNotFound, "Failed to parse blockheader: %s.", objecthash)
		}

		// check version
		if !bytes.Equal(blockHeader.Version, []byte(HEADER_VERSION)) {
			return SwarmErrorf(http.StatusNotFound, "Invalid blockheader version: %s", blockHeader.Version)
		}

		//// set del object to not found
		//if blockHeader.State == HEADER_STATE_DEL {
		//	return SwarmErrorf(http.StatusNotFound, "Object %s not exists.", objecthash)
		//}

		// verify sha1
		if objecthash != Sha1Hex((*pdata)[HEADER_SIZE:]) {
			return SwarmErrorf(http.StatusNotFound, "Mismatch verify objecthash: %s", objecthash)
		}

		// set header Content-Length
		objectsize := len(*pdata) - HEADER_SIZE
		writer.Header().Set("Content-Length", Itoa(int64(objectsize)-offset))

		// send response
		writer.Write((*pdata)[HEADER_SIZE+offset:])

		// finish all blocks
		logger.Info("Finish read object %s", objecthash)
	}

	return nil
}

// Dispatch block readers in parallel. Num of readers will not exceed maxQueue.
func dispatchReadBlock(blockChan chan QueueReadBlockResult, pqueueLength *int32, blockhashes []string) {
	defer onRecoverablePanic()

	numBlock := len(blockhashes)
	indexDispatch := 0
	for {
		if indexDispatch >= numBlock {
			// finish
			break
		}

		if *pqueueLength < 1 {
			// empty queue
			time.Sleep(1e7)
			continue
		}

		// start new reader
		blockhash := blockhashes[indexDispatch]
		go queueReadBlock(blockChan, blockhash, indexDispatch)

		// adjust controller
		atomic.AddInt32(pqueueLength, -1)
		indexDispatch += 1
	}
}

// Queue read block
func queueReadBlock(blockChan chan QueueReadBlockResult, blockhash string, indexDispatch int) {
	defer onRecoverablePanic()

	result := QueueReadBlockResult{
		blockhash: blockhash,
		index:     indexDispatch,
		status:    STATUS_OK,
	}

	pdata, err := readBlockInDomain(blockhash, BLOCK_TYPE_BLOCK, false, STATUS_PROXIED)
	result.pdata = pdata
	if err != nil {
		result.status = STATUS_ERROR
	}
	blockChan <- result
}

// Parse blockhashes for objectsize and joinhashes
func parseBlockhashes(objecthash string, pdata *[]byte) (objectsize int64, joinhashes string, blockHeader BlockHeader, err error) {
	blockHeader, err = LoadBlockHeader((*pdata)[0:HEADER_SIZE])
	if err != nil {
		logger.Error("Failed blockHeader: %x, error: %s", (*pdata), err.Error())
		err = fmt.Errorf("Failed to parse blockheader: %s.", objecthash)
		return
	}

	// check version
	if !bytes.Equal(blockHeader.Version, []byte(HEADER_VERSION)) {
		err = fmt.Errorf("Invalid blockheader version: %s", blockHeader.Version)
		return
	}

	// verify sha1
	if objecthash != Sha1Hex((*pdata)[HEADER_SIZE:]) {
		err = fmt.Errorf("Mismatch verify objecthash: %s", objecthash)
		return
	}

	// verify joinhashes length
	joinhashes = string((*pdata)[HEADER_SIZE:])
	length := len(joinhashes)
	if length%HASH_HEX_SIZE != 0 {
		err = fmt.Errorf("Invalid joinhashes length: %d", length)
		return
	}

	// verify objectsize
	objectsize = blockHeader.Size
	if int64(length/HASH_HEX_SIZE*BLOCK_SIZE) < objectsize || int64((length/HASH_HEX_SIZE-1)*BLOCK_SIZE) > objectsize {
		err = fmt.Errorf("Invalid objectsize: %d", objectsize)
		return
	}

	return objectsize, joinhashes, blockHeader, nil
}

// Read block of local domain
// Try read block from hashring with proxied. 
// If failed, try read from local block server with proxied.
// TODO: need more test
func readBlockInDomain(blockhash string, blocktype string, onlyheader bool, proxied string) (pdata *[]byte, err error) {
	// choice first node
	var nodeIp, deviceId string
	nodes, err := DOMAIN_RING.GetReplicaNodes(blockhash, 1, true)
	if err != nil {
		return
	}
	for nodeIp, deviceId = range nodes {
		break
	}

	// gather block from first node
	gatherChan := make(chan ReadBlockResult)
	blockInfo := NewBlockInfo(deviceId, blocktype, blockhash)
	go gatherBlock(gatherChan, nodeIp, blockInfo, onlyheader, proxied)

	select {
	case result := <-gatherChan:
		if result.status != STATUS_OK {
			msg := fmt.Sprintf("Failed gather block %s from %s", blockInfo, nodeIp)
			logger.Error(msg)
			RecordAccessResult(nodeIp, ACTION_READ, false)

			if nodeIp == NODE_ADDRESS {
				// no retry for local
				return nil, fmt.Errorf(msg)
			}

			// retry local block server
			// make new chan
			//close(gatherChan)
			gatherChan = make(chan ReadBlockResult)
			blockAnyDevice := NewBlockAnyDevice(blockInfo.Type, blockInfo.Hash)
			go gatherBlock(gatherChan, NODE_ADDRESS, blockAnyDevice, onlyheader, proxied)
			select {
			case result := <-gatherChan:
				if result.status != STATUS_OK {
					return nil, fmt.Errorf("Failed gather block %s from local server with error status", blockAnyDevice)
				}
				RecordAccessResult(nodeIp, ACTION_READ, true)
				return result.pdata, nil
			}
		}
		RecordAccessResult(nodeIp, ACTION_READ, true)
		return result.pdata, nil

	case <-time.After(GATHER_BLOCK_TIMEOUT):
		msg := fmt.Sprintf("Timeout gather block %s from %s", blockInfo, nodeIp)
		logger.Error(msg)
		RecordAccessResult(nodeIp, ACTION_READ, false)

		if nodeIp == NODE_ADDRESS {
			// no retry for local
			return nil, fmt.Errorf(msg)
		}

		// retry local block server
		//close(gatherChan)
		gatherChan = make(chan ReadBlockResult)
		blockAnyDevice := NewBlockAnyDevice(blockInfo.Type, blockInfo.Hash)
		go gatherBlock(gatherChan, NODE_ADDRESS, blockAnyDevice, onlyheader, proxied)
		select {
		case result := <-gatherChan:
			if result.status != STATUS_OK {
				return nil, fmt.Errorf("Failed gather block %s from local server with error status", blockAnyDevice)
			}
			RecordAccessResult(nodeIp, ACTION_READ, true)
			return result.pdata, nil
		}
	}
	err = fmt.Errorf("Error")
	return
}

// Get block from block server
func gatherBlock(gatherChan chan ReadBlockResult, nodeIp string, blockInfo BlockInfo, onlyheader bool, proxied string) {
	defer onRecoverablePanic()

	result := ReadBlockResult{
		blockhash: blockInfo.Hash,
		status:    STATUS_OK,
	}

	if nodeIp == "" || !blockInfo.Verify() {
		logger.Error("Invalid param for gather block %s node %s", blockInfo, nodeIp)
		result.status = STATUS_ERROR
		gatherChan <- result
		return
	}

	// receive block
	pdata, err := requestReadBlock(nodeIp, blockInfo, onlyheader, proxied)
	if err != nil {
		logger.Error("Failed read block %s from %s, error: %s", blockInfo, nodeIp, err.Error())
		result.status = STATUS_ERROR
		gatherChan <- result
		return
	}

	if !onlyheader {
		// verify
		_, err = verifyBlock(blockInfo.Hash, blockInfo.Type, pdata)
		if err != nil {
			ALARM_MANAGER.Add(ALARM_MISMATCH_BLOCKHASH, err, "Mismatch hash, block %s on node %s", blockInfo, nodeIp)
			result.status = STATUS_ERROR
			gatherChan <- result
			return
		}
	}

	// fill block data
	result.pdata = pdata

	// notify
	gatherChan <- result
}

// Handle query object

type ObjectStatusResponse struct {
	Version    string
	Timestamp  time.Time
	Status     int
	CTime      time.Time
	ObjectSize int64
}

func handleQueryObject(writer http.ResponseWriter, req *http.Request) *SwarmError {
	MONITOR.SetVariable("M:FS:QueryObject", 1)

	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"objecthash", verifyObjecthash},
		ParamVerifier{"blocktype", verifyObjectBlocktype},
	})
	if swarmError != nil {
		return swarmError
	}
	objecthash, blocktype := params[0], params[1]

	//logger.Debug("handleQueryObject objecthash %s from %s", objecthash, IPFromAddr(req.RemoteAddr))

	// build response
	response := ObjectStatusResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	// read objecthash
	pdata, err := readBlockInDomain(objecthash, blocktype, true, STATUS_PROXIED)
	if err != nil {
		return SwarmErrorf(http.StatusNotFound, err.Error())
	}
	blockHeader, err := LoadBlockHeader(*pdata)
	if err != nil {
		err = fmt.Errorf("Failed to parse blockheader: %s.", objecthash)
		return SwarmErrorf(http.StatusNotFound, err.Error())
	}
	// set del object to not found
	if blockHeader.State == HEADER_STATE_DEL {
		return SwarmErrorf(http.StatusNotFound, "Object %s not exists.", objecthash)
	}

	// update response
	response.CTime = blockHeader.Timestamp
	response.ObjectSize = blockHeader.Size

	// finish
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}

	logger.Info("Query object %s status %d", objecthash, response.Status)
	return nil
}
