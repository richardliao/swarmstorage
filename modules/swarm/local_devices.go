package swarm

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

/*
About disk failure:

Hard disk MTTF is about 30,000 hours. A big domain may have 50,000 disks. That
means there is a disk failed in about half an hour. 

How to detect disk failure:

smartctl: tool to monitor disk health.

According to "Failure Trends in a Large Disk Drive Population", over 36% of all 
failed drives had zero SMART error.

It is admin's duty to find out disk abnormal and make decision. There are some
helpers to help admins to do that. 
 Alarm when disk read/write failed. 
 Admin APIs can be used to offline/remove/online disks. 
 TODO: Admin toolkit to clone and replace failed disk.
*/

//------------------------------------------------------------------------------
// DeviceState
//------------------------------------------------------------------------------

/*
Device status:
Online: both remote and local will consider it online
Offline: remote will consider it as online, 
        local will read/push repair/remote write as online,
        local write/pull repair/gc as removed.
Removed: both remote and local will consider it removed (do not umount directly)

online <-> offline by admin --+
       <-> remove by admin <-+
*/

type DeviceState struct {
	Weight int
	Path   string // mount path
	Status string // online, offline, removed
}

func NewDeviceState(weight int, devicePath string, status string) (this *DeviceState) {
	this = &DeviceState{
		Weight: weight,
		Path:   devicePath,
		Status: status,
	}
	return
}

func (this *DeviceState) String() string {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	buffer.WriteString("Weight")
	buffer.WriteString(": ")
	buffer.WriteString(fmt.Sprintf("%d", this.Weight))
	buffer.WriteString(",")
	buffer.WriteString("Status")
	buffer.WriteString(": ")
	buffer.WriteString(this.Status)
	buffer.WriteString("}")
	return buffer.String()
}

func (this *DeviceState) Equal(deviceState *DeviceState) bool {
	// status is not compared, since offline device will not propogate out of 
	// local node, and response will not include removed devices.
	if this.Weight == deviceState.Weight {
		if this.Status == deviceState.Status {
			return true
		}
		if this.Status == DEVICE_STATUS_OFFLINE && deviceState.Status == DEVICE_STATUS_ONLINE {
			// offline equals online
			return true
		}
		if deviceState.Status == DEVICE_STATUS_OFFLINE && this.Status == DEVICE_STATUS_ONLINE {
			// offline equals online
			return true
		}
	}
	return false
}

//------------------------------------------------------------------------------
// LocalDevices
//------------------------------------------------------------------------------

/*
LocalDevices will remember all devices in runtime, include removed devices.
Umount a device will not delete it, just change its status to removed. 

Since device status is not persistent, restart swarm will lose all devices 
status specified by admin, i.e. offline/removed devices will be online again, and 
umounted devices will have no record. 

It is admin's duty to physically umount unwanted devices before restart.
*/

type LocalDevices struct {
	lock *sync.RWMutex
	Data map[string]*DeviceState // {deviceId: DeviceState}
}

func NewLocalDevices() (this *LocalDevices) {
	this = &LocalDevices{
		lock: new(sync.RWMutex),
		Data: make(map[string]*DeviceState),
	}
	return
}

// Local online devices
func (this *LocalDevices) LocalOnlineDevices() (nodeDevices *NodeDevices) {
	nodeDevices = NewNodeDevices()

	this.lock.RLock()
	defer this.lock.RUnlock()

	for deviceId, deviceState := range this.Data {
		if deviceState.Status == DEVICE_STATUS_ONLINE {
			nodeDevices.Set(deviceId, deviceState.Weight)
		}
	}
	return
}

// Number of online and offline devices
func (this *LocalDevices) LocalDeviceNum() (i int) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	for _, deviceState := range this.Data {
		if deviceState.Status != DEVICE_STATUS_REMOVED {
			i++
		}
	}
	return
}

// Check if device is local and online or offline(i.e., not removed)
func (this *LocalDevices) IsLocalDevice(deviceId string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()

	if deviceState, ok := this.Data[deviceId]; ok {
		if deviceState.Status != DEVICE_STATUS_REMOVED {
			return true
		}
	}
	return false
}

// Check if device is local and online
func (this *LocalDevices) IsLocalOnlineDevice(deviceId string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()

	if deviceState, ok := this.Data[deviceId]; ok {
		if deviceState.Status == DEVICE_STATUS_ONLINE {
			return true
		}
	}
	return false
}

// Not include removed devices
func (this *LocalDevices) LocalDevicePath(deviceId string) (devicePath string, ok bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	deviceState, ok := this.Data[deviceId]
	if !ok {
		return
	}
	if deviceState.Status != DEVICE_STATUS_REMOVED {
		devicePath = deviceState.Path
		ok = true
	} else {
		ok = false
	}
	return
}

// Include removed
func (this *LocalDevices) GetState(deviceId string) (deviceState *DeviceState, ok bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	deviceState, ok = this.Data[deviceId]
	return
}

// Change device state
func (this *LocalDevices) SetState(deviceId string, deviceState *DeviceState) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.Data[deviceId] = deviceState
}

// Get device status
func (this *LocalDevices) GetStatus(deviceId string) (status string) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	// get status from LocalDevices
	deviceState, ok := this.Data[deviceId]
	if !ok {
		// non-exist device default is online
		status = DEVICE_STATUS_ONLINE
	} else {
		status = deviceState.Status
	}

	// apply status from NodeSettings
	if adminStatus, ok := NODE_SETTINGS.Devices[deviceId]; ok {
		status = adminStatus
	}
	return
}

// Change device status, which is changed by admin or by umount directly
func (this *LocalDevices) SetStatus(deviceId string, status string) {
	this.lock.Lock()
	defer this.lock.Unlock()

	deviceState, ok := this.Data[deviceId]
	if !ok {
		// skip when device not exist
		return
	}
	deviceState.Status = status
}

// Change device weight
func (this *LocalDevices) SetWeight(deviceId string, weight int) {
	this.lock.Lock()
	defer this.lock.Unlock()

	deviceState, ok := this.Data[deviceId]
	if !ok {
		// skip when device not exist
		return
	}
	deviceState.Weight = weight
}

// Change device path
func (this *LocalDevices) SetPath(deviceId string, path string) {
	this.lock.Lock()
	defer this.lock.Unlock()

	deviceState, ok := this.Data[deviceId]
	if !ok {
		// skip when device not exist
		return
	}
	deviceState.Path = path
}

// Get local devices
// devices: {deviceIc: DeviceState}, with status from current
func (this *LocalDevices) Scan() (devices map[string]*DeviceState) {
	devicesPath := filepath.Join(NODE_BASE_PATH, DEVICES_DIR)
	files, err := ioutil.ReadDir(devicesPath)
	checkFatal(err)

	devices = make(map[string]*DeviceState)

	for _, file := range files {
		switch {
		case file.IsDir():
			if !strings.HasPrefix(file.Name(), DEVICE_PREFIX) {
				continue
			}

			devicePath := filepath.Join(devicesPath, file.Name())
			uuidPath := filepath.Join(devicePath, UUID_FILE)
			uuidFile, err := os.Open(uuidPath)
			if err != nil {
				continue
			}

			data := make([]byte, 100)
			count, err := uuidFile.Read(data)
			if err != nil {
				continue
			}
			deviceId := strings.ToLower(strings.TrimSpace(string(data[:count])))
			if len(deviceId) != 32 {
				continue
			}
			if _, ok := devices[deviceId]; ok {
				ALARM_MANAGER.Add(ALARM_DUPLICATE_DEVICE, fmt.Errorf(""), "Duplicate device %s", deviceId)
				continue
			}

			// get weight
			diskspace := getDiskspace(devicePath)
			weight := int(math.Floor(float64(diskspace) / DEVICE_WEIGHT_UNIT))
			if weight == 0 {
				// not qualified device
				continue
			}

			// found
			devices[deviceId] = NewDeviceState(weight, devicePath, this.GetStatus(deviceId))
		}
	}

	return devices
}

// Update local devices
func (this *LocalDevices) Update(devices map[string]*DeviceState) {

	// throttle
	throttle("updateLocalDevices", "")

	MONITOR.SetVariable("M:GOS:UpdateLocalDevices", 1)

	changed := false

	// update using devices
	for deviceId, candidateState := range devices {
		deviceState, ok := this.GetState(deviceId)
		if !ok {
			// add device
			this.SetState(deviceId, candidateState)
			changed = true
			continue
		}

		if deviceState.Weight != candidateState.Weight || deviceState.Path != candidateState.Path {
			// update weight and path when changed, even for removed devices
			this.SetWeight(deviceId, candidateState.Weight)
			this.SetPath(deviceId, candidateState.Path)

			if deviceState.Status != DEVICE_STATUS_REMOVED {
				// publish when device not removed
				changed = true
			}
		}
		// device exists and not change
	}

	// find missing devices, that may be umounted directly
	for deviceId, deviceState := range this.Data {
		if deviceState.Status == DEVICE_STATUS_REMOVED {
			// removed already
			continue
		}
		if _, ok := devices[deviceId]; !ok {
			//delete missing device
			if _, ok := this.GetState(deviceId); !ok {
				// skip when device not exist
				continue
			}
			this.SetStatus(deviceId, DEVICE_STATUS_REMOVED)

			changed = true
		}
	}

	if changed {
		this.OnChanged()
	}
}

// Update domain map, device manager and notify correlate nodes
func (this *LocalDevices) OnChanged() {
	// update domain map
	localNodeState := DOMAIN_MAP.LocalNodeState()
	for deviceId, deviceState := range this.Data {
		if deviceState.Status == DEVICE_STATUS_REMOVED {
			// skip removed
			continue
		}
		weight, ok := localNodeState.Devices.Get(deviceId)
		if !ok || *weight != deviceState.Weight {
			// add device or change weight, even removed
			localNodeState.SetDevice(deviceId, deviceState.Weight)
			continue
		}
	}
	for deviceId := range localNodeState.Devices.Data {
		// find missing devices, that may be umounted directly
		if deviceState, ok := this.GetState(deviceId); !ok || deviceState.Status == DEVICE_STATUS_REMOVED {
			//delete missing device
			localNodeState.DeleteDevice(deviceId)
		}
	}

	// domain map OnChanged
	changedNodes := DOMAIN_MAP.OnChanged()
	//logger.Debug("changedNodes %#v", *changedNodes.Data)

	// update DEVICE_MANAGER
	DEVICE_MANAGER.Update()

	// notify affected nodes to probe this node to update domain map
	for nodeIp := range changedNodes.Data {
		go requestGossipNotify(nodeIp)
	}

	// save settings
	NODE_SETTINGS.Save()
}
