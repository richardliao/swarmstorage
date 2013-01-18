package swarm

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"
)

//------------------------------------------------------------------------------
// DeviceManager
//------------------------------------------------------------------------------

/*
Coordinate read/write requests for a device, do batch read or write request to 
increase disk throughput.
*/
type DeviceManager map[string]DevicePiper // {deviceId: DevicePiper}

func NewDeviceManager() DeviceManager {
	return make(map[string]DevicePiper)
}

// Update devices and start/stop device pipers
func (this *DeviceManager) Update() {
	// stop piper of removed devices
	for deviceId, deviceState := range LOCAL_DEVICES.Data {
		if deviceState.Status == DEVICE_STATUS_REMOVED {
			if piper, ok := (*this)[deviceId]; ok {
				piper.Stop()
			}
		}
	}

	// start piper for new devices
	for deviceId := range LOCAL_DEVICES.Data {
		if _, ok := (*this)[deviceId]; ok {
			// skip exist
			continue
		}

		piper := DevicePiper{
			deviceId: deviceId,
			// limit queue length
			// TODO: caller should handle busy error to avoid misleading
			readChan:  make(chan ReadFileRequest, READ_QUEUE_LENGTH),
			writeChan: make(chan WriteFileRequest, WRITE_QUEUE_LENGTH),
		}
		go piper.Start()
		(*this)[deviceId] = piper
	}
}

// Read
func (this *DeviceManager) ReadFile(blockInfo BlockInfo, offset int64, size int) (pdata *[]byte, err error) {
	devicePiper, ok := (*this)[blockInfo.Device]
	if !ok {
		err = fmt.Errorf("No such device %s", blockInfo.Device)
		return
	}
	pdata, err = devicePiper.ReadFile(blockInfo, offset, size)
	if err != nil {
		if isAlarmIoError(err) {
			ALARM_MANAGER.Add(ALARM_DEVICE_IOERROR, err, "Failed piper read file %s", blockInfo)
		}
		return
	}
	return pdata, nil
}

// Write
func (this *DeviceManager) WriteFile(blockInfo BlockInfo, offset int64, pdata *[]byte) (err error) {
	devicePiper, ok := (*this)[blockInfo.Device]
	if !ok {
		err = fmt.Errorf("No such device %s", blockInfo.Device)
		return
	}
	err = devicePiper.WriteFile(blockInfo, offset, pdata)
	if err != nil {
		if isAlarmIoError(err) {
			ALARM_MANAGER.Add(ALARM_DEVICE_IOERROR, err, "Failed piper write file %s", blockInfo)
		}
		return
	}
	return nil
}

//------------------------------------------------------------------------------
// DevicePiper
//------------------------------------------------------------------------------

type DevicePiper struct {
	deviceId  string
	readChan  chan ReadFileRequest
	writeChan chan WriteFileRequest
}

type ReadFileRequest struct {
	respChan  chan ReadFileResponse
	blockInfo BlockInfo
	offset    int64
	size      int
}

type ReadFileResponse struct {
	pdata *[]byte
	err   error
}

type WriteFileRequest struct {
	respChan  chan WriteFileResponse
	blockInfo BlockInfo
	offset    int64
	pdata     *[]byte
}

type WriteFileResponse struct {
	err error
}

// Handle request from chan, do batch read/write.
// Read have higher priority than write.
// TODO: keepalive
func (this *DevicePiper) Start() {
	readCounter := 0
	writeCounter := 0

READ:
	for {
		select {
		case req := <-this.readChan:
			readCounter++
			req.respChan <- this.doReadFile(req)

			if readCounter > 10 {
				// reset counter
				readCounter = 0

				// prevent starve writeChan
				select {
				case req := <-this.writeChan:
					req.respChan <- this.doWriteFile(req)
				default:
					continue READ
				}
			}
		default:
			// if nothing to read, do write
			writeCounter = 0
			for {
				// batch write
				select {
				case req := <-this.writeChan:
					writeCounter++
					req.respChan <- this.doWriteFile(req)

					if writeCounter > 5 {
						// prevent starve readChan
						continue READ
					}
				default:
					// no more write, sleep a while
					time.Sleep(1e7)
					continue READ
				}
			}
		}
	}
}

// TODO:
func (this *DevicePiper) Stop() {
}

// Make a read request and wait for result in resp chan
func (this *DevicePiper) ReadFile(blockInfo BlockInfo, offset int64, size int) (pdata *[]byte, err error) {
	if len(this.readChan) == cap(this.readChan) {
		err = fmt.Errorf("Device %s too busy for read", this.deviceId)
		return
	}

	respChan := make(chan ReadFileResponse)
	req := ReadFileRequest{
		respChan:  respChan,
		blockInfo: blockInfo,
		offset:    offset,
		size:      size,
	}

	// send request
	this.readChan <- req

	// wait resp
	resp := <-respChan

	pdata, err = resp.pdata, resp.err
	return
}

// Do read file
// size 0 means read whole file
func (this *DevicePiper) doReadFile(req ReadFileRequest) (resp ReadFileResponse) {
	resp = ReadFileResponse{
		pdata: nil,
		err:   nil,
	}
	blockPath, err := getBlockPath(req.blockInfo)
	if err != nil {
		resp.err = err
		return
	}

	var data []byte
	if req.size == 0 && req.offset == 0 {
		// read whole file
		data, err = ioutil.ReadFile(blockPath)
		if err != nil {
			resp.err = err
			return
		}
	} else {
		// read part file
		f, err := os.OpenFile(blockPath, os.O_RDONLY, 0666)
		if err != nil {
			resp.err = err
			return
		}

		// seek
		if req.offset != 0 {
			ret, err := f.Seek(req.offset, os.SEEK_SET)
			if err != nil {
				resp.err = err
				return
			}
			if ret != req.offset {
				err = fmt.Errorf("Seek failed for %s offset %s", blockPath, req.offset)
				resp.err = err
				return
			}
		}

		// read
		data = make([]byte, req.size)
		n, err := f.Read(data)
		if err != nil {
			resp.err = err
			return
		}
		if n != req.size {
			data = data[0:n]
		}
	}
	MONITOR.SetVariable("M:DISK:Read", len(data))
	resp.pdata = &data
	return
}

// Make a write request and wait for result in resp chan
func (this *DevicePiper) WriteFile(blockInfo BlockInfo, offset int64, pdata *[]byte) (err error) {
	if len(this.writeChan) == cap(this.writeChan) {
		err = fmt.Errorf("Device %s too busy for write", this.deviceId)
		return
	}

	respChan := make(chan WriteFileResponse)
	req := WriteFileRequest{
		respChan:  respChan,
		blockInfo: blockInfo,
		offset:    offset,
		pdata:     pdata,
	}

	// send request
	this.writeChan <- req

	// wait resp
	resp := <-respChan

	err = resp.err
	return
}

// Do write file
func (this *DevicePiper) doWriteFile(req WriteFileRequest) (resp WriteFileResponse) {
	resp = WriteFileResponse{
		err: nil,
	}

	blockPath, err := getBlockPath(req.blockInfo)
	if err != nil {
		resp.err = err
		return
	}

	// prepare dir
	blockDir := path.Dir(blockPath)
	if !ExistPath(blockDir) {
		devicePath, ok := LOCAL_DEVICES.LocalDevicePath(this.deviceId)
		if !ok || !ExistPath(devicePath) {
			// never make device dir in case device umounted
			resp.err = fmt.Errorf("Device %s not exist.", devicePath)
			return
		}

		// path not exist
		err = os.MkdirAll(blockDir, DIR_MOD)
		if err != nil {
			logger.Error("Failed create dir: %s", blockDir)
			resp.err = err
			return
		}
	}

	// TODO: offset
	if req.offset != 0 {
		resp.err = fmt.Errorf("Not implement.")
		return
	}
	err = ioutil.WriteFile(blockPath, *req.pdata, FILE_MOD)
	if err != nil {
		resp.err = err
		return
	}
	MONITOR.SetVariable("M:DISK:Write", len(*req.pdata))
	return
}

// Check if need alarm 
func isAlarmIoError(err error) (needAlarm bool) {
	switch t := err.(type) {
	case *os.PathError:
		if os.IsNotExist(t.Err) {
			return false
		}
	}
	return true
}
