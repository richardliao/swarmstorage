package swarm

import (
	"bytes"
	"fmt"
	"sync"
	"time"
)

/*
Maintain domain map:
 Member node have their own perspective of the domain, and use it to cooperate 
 with other nodes, following their common rule. 

 But their perspective lacks of comprehensive knowledge of the whole domain.

 It need higher level intelligence to decide what to do to recover from bad 
 situation. This is done by admin API.

 Although domain can endure bad situation, but in low efficiency. Good 
 maintainence can greatly improve efficiency.

 Admin should keep an eye on nodes long behind actual domain map and fix them 
 quickly.

 Admin should decide what to do when alarm of node down occurs(blocks on down 
 node will NOT be repaired by rebalance, therefore is insufficient for replica of 
 running nodes).

Node status: 
 Up: in ring, can access
 Removed: not in ring, no access
 Down: in ring, no access. 

Access include: read/write/rebalance/gc

Functions:
 Domain map: build ring, include down but not removed nodes. (Expected to be 
 stable to avoid ring changes frequently.) 
 Ring: include down for rebalance, exclude down for read/write. 
 Gossip: update node status. 

Data is safe when node down:
 Write to down node will be relayed to handoff node. 
 Read can choose from any available replicas. 
 Rebalance will skip the down node until it up and stable. 
 There are enough replicas(on up, down and handoff nodes) for a block.

 When node is out for very long time, repair blocks. The problem is if this is 
 just one node's perspective, blocks will be repaired and gced infinitely. So, 
 the reciever node should check if repaired node out when the tick does not 
 belong to the reciever, i.e. the reciever is the right relayer. Node out state 
 is not expected to be consistent among correlated nodes, and can not take out 
 state same as removed. 
    TODO: To prevent the reciever's disk space from been over used, reciever 
    should disperse the handoff blocks to other local devices. The problem is
    that will not be rebalanced quickly and gc slowly.

Add/remove node:
 Add: New node join domain immediately without admin.
 Remove: admin notify an available node a node is removed, with intention that 
 node will not be back again. 
 Resume: removed node will not rejoin until admin add it back before cleanup, 
 admin notify the resuming node.
 Cleanup: removed node will be cleaned up after an extended period of time

Effections of add/remove node:
 Read/write: have instant effect, can tolerate frequent changes of ring
 Rebalance: batch add/remove devices, cause batch block migration and expensive.
 Available: consider boot up time, take unreliable node as down. compare node 
 timestamp to find if node just reboot.
 Reliable: each node remember other nodes status/access history(reliability 
 reputation) for tuning read/write target.

    TODO: toolkit to mitigate the migration penalty by preparing blocks before 
    add/resume node

Decide node status by its behaviour, not what it announces.
*/

//------------------------------------------------------------------------------
// NodeDevices
//------------------------------------------------------------------------------

type NodeDevices struct {
	lock *sync.RWMutex
	Data map[string]*int // {deviceId: weight}
}

func NewNodeDevices() (this *NodeDevices) {
	return &NodeDevices{
		lock: new(sync.RWMutex),
		Data: make(map[string]*int),
	}
}

func (this *NodeDevices) String() string {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	for deviceId, weight := range this.Data {
		buffer.WriteString(deviceId)
		buffer.WriteString(": ")
		buffer.WriteString(fmt.Sprintf("%d", *weight))
		buffer.WriteString(",")
	}
	buffer.WriteString("}")
	return buffer.String()
}

func (this *NodeDevices) Get(deviceId string) (weight *int, ok bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	weight, ok = this.Data[deviceId]
	return
}

// Add a device with weight
func (this *NodeDevices) Set(deviceId string, weight int) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.Data[deviceId] = &weight
}

func (this *NodeDevices) Delete(deviceId string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.Data, deviceId)
}

// Compare 
func (this *NodeDevices) Equal(devices2 *NodeDevices) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()

	if this.Data == nil {
		if devices2.Data == nil {
			return true
		}
		return false
	}

	if len(this.Data) != len(devices2.Data) {
		return false
	}

	for deviceId, weight1 := range this.Data {
		weight2, ok := devices2.Data[deviceId]
		if !ok || *weight1 != *weight2 {
			return false
		}
	}
	return true
}

// Empty
func (this *NodeDevices) Empty() {
	this.lock.Lock()
	defer this.lock.Unlock()
	for deviceId := range this.Data {
		delete(this.Data, deviceId)
	}
}

//------------------------------------------------------------------------------
// NodeState
//------------------------------------------------------------------------------

/*
JoinTime and RemoveTime is used as version of simplified vector clock, which not 
record origin node but just the highest version. The value (Status) and version 
will be replaced by remote one of probing if remote one have higher version, 
comparing both JoinTime and RemoveTime and choose higher version thus Status.

JoinTime is admin join timestamp of origin node clock, or zero if node is newly 
found. It will keep saving the highest version of further probe result.

RemoveTime is admin remove timestamp of any node clock, and keep update like 
JoinTime.

DownTime is local node timestamp of last remote node down. 

There are actually two type of status: 
 join/remove status: calc by RemoveTime and JoinTime
 up/down status: calc by DownTime and JoinTime only when joined

Join/remove status has domain scope affect and maintained by admin and auto 
discovery. New node is auto join without manual admin. A node will be kept 
joined even down for long time(will got alarm anyway). Removed node will be kept
from joined until admin join it, or until all nodes have cleared the removed 
record(that is very long time).

Up/down status has local affect and maintained locally.

The final status is the composition of above status: up, down and removed
*/

type NodeState struct {
	JoinTime   time.Time    // origin clock, to admin resume
	RemoveTime time.Time    // admin clock, to cleanup and keep node from rejoin without admin
	DownTime   time.Time    // local clock, time found node down, to alarm
	StartTime  time.Time    // origin clock, the node startup time, to guess available time
	Devices    *NodeDevices // {deviceId: NodeDevices}
}

func (this *NodeState) String() string {
	//format := "2006-01-02T15:04:05"
	return fmt.Sprintf("{%s %s}", this.Status(), this.Devices)
}

func NewNodeState() (this *NodeState) {
	this = &NodeState{
		JoinTime:   time.Time{},
		RemoveTime: time.Time{},
		DownTime:   time.Time{},
		StartTime:  time.Time{},
		Devices:    NewNodeDevices(),
	}
	return
}

// Delete device
func (this *NodeState) DeleteDevice(deviceId string) {
	this.Devices.Delete(deviceId)
}

// Set device
func (this *NodeState) SetDevice(deviceId string, weight int) {
	this.Devices.Set(deviceId, weight)
}

// Record node down
func (this *NodeState) RecordDown() {
	if this.Status() == NODE_STATUS_REMOVED {
		return
	}
	this.DownTime = time.Now()
}

// Reset node down
func (this *NodeState) RecordUp() (changed bool) {
	changed = false
	if this.DownTime.After(time.Time{}) {
		this.DownTime = time.Time{}
		changed = true
	}
	return
}

// Admin remove node
func (this *NodeState) AdminRemove() {
	this.RemoveTime = time.Now()

	// clear devices
	this.Devices.Empty()
}

// Admin join node join
func (this *NodeState) AdminJoin() {
	this.JoinTime = time.Now()
}

//// Join new discovered node
//func (this *NodeState) NewJoin(verifiedNodeState *NodeState) {
//	this.JoinTime = time.Time{}
//	this.Devices = verifiedNodeState.Devices
//}

// Calc node status
func (this *NodeState) Status() (status string) {
	// get result status
	if this.RemoveTime.After(this.JoinTime) {
		status = NODE_STATUS_REMOVED
	} else {
		if this.DownTime.After(this.JoinTime) {
			status = NODE_STATUS_DOWN
		} else {
			status = NODE_STATUS_UP
		}
	}
	return
}

// Merge latest state, return join change
func (this *NodeState) MergeStatus(newState *NodeState) (changed bool) {
	changed = false

	// save current status
	oldStatus := this.Status()

	// merge the highest version
	if newState.RemoveTime.After(this.RemoveTime) {
		this.RemoveTime = newState.RemoveTime
	}
	if newState.JoinTime.After(this.JoinTime) {
		this.JoinTime = newState.JoinTime
	}
	if newState.StartTime.After(this.StartTime) {
		this.StartTime = newState.StartTime
	}

	// compare new status
	if this.Status() != oldStatus {
		changed = true
	}

	return
}

//------------------------------------------------------------------------------
// DomainMap
//------------------------------------------------------------------------------

type DomainMap struct {
	lock *sync.RWMutex
	Data map[string]*NodeState // {nodeIp: NodeState}
}

func NewDomainMap() (this *DomainMap) {
	this = &DomainMap{
		lock: new(sync.RWMutex),
		Data: make(map[string]*NodeState),
	}
	return
}

func (this *DomainMap) String() (s string) {
	for nodeIp, nodeState := range this.Data {
		s += fmt.Sprint(nodeIp, ": ", *nodeState, ", ")
	}
	return
}

func (this *DomainMap) Get(nodeIp string) (nodeState *NodeState, ok bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	nodeState, ok = this.Data[nodeIp]
	return
}

func (this *DomainMap) Set(nodeIp string, nodeState *NodeState) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.Data[nodeIp] = nodeState
}

func (this DomainMap) Delete(nodeIp string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.Data, nodeIp)
}

// Record down node
func (this *DomainMap) RecordDownNode(nodeIp string) {
	currentNodeState, ok := this.Get(nodeIp)
	if !ok {
		// not known before, just ignore
		// and it is important to cleanup obsolete nodes
		return
	}

	if currentNodeState.Status() == NODE_STATUS_DOWN {
		// already down
		return
	}

	// update status
	currentNodeState.RecordDown()

	logger.Info("Found node %s down, new NodeState %s", nodeIp, currentNodeState)

	this.OnChanged()
}

// Merge node states
func (this *DomainMap) MergeDomainMap(nodeIp string, remoteDomainMap *DomainMap) {
	changed := false // include status change and devices change

	// origin node is up
	if nodeState, ok := this.Get(nodeIp); ok {
		changed = nodeState.RecordUp()

		// reset alarm of node down
		ALARM_MANAGER.DelByTitle(fmt.Sprintf("Node %s is out", nodeIp))

		// reset rebalance out
		REBALANCE_CACHE.Out.Delete(nodeIp)
	}

	for remoteIp, remoteNodeState := range remoteDomainMap.Data {
		if remoteNodeState == nil {
			// invalid
			continue
		}

		//if remoteIp == NODE_ADDRESS {
		//	// skip local node
		//	continue
		//}

		currentNodeState, ok := this.Get(remoteIp)
		if !ok {
			// avoid nil lock
			remoteNodeState.Devices.lock = new(sync.RWMutex)
			// new node, use remote
			this.Set(remoteIp, remoteNodeState)
			changed = true
			continue
		}

		oldStatus := currentNodeState.Status()

		// merge status
		joinChanged := currentNodeState.MergeStatus(remoteNodeState)
		if joinChanged {
			changed = true
		}

		// check device change
		devicesChanged := !currentNodeState.Devices.Equal(remoteNodeState.Devices)
		if devicesChanged {
			changed = true
		}

		if !joinChanged && !devicesChanged && currentNodeState.Status() == oldStatus && remoteIp != nodeIp {
			// no change and not origin node
			continue
		}

		// verify origin node in spite of if changed

		// verify node status
		verifiedNodeState, err := requestGossipVerify(remoteIp)
		if err != nil {
			if currentNodeState.Status() != NODE_STATUS_REMOVED {
				// record down
				currentNodeState.RecordDown()
			}
			continue
		}

		// node is up
		if currentNodeState.RecordUp() {
			changed = true
			// reset alarm of node down
			ALARM_MANAGER.DelByTitle(fmt.Sprintf("Node %s is out", nodeIp))

			// reset rebalance out
			REBALANCE_CACHE.Out.Delete(nodeIp)
		}

		// merge verified status
		currentNodeState.MergeStatus(verifiedNodeState)

		// update devices
		if currentNodeState.Status() == NODE_STATUS_REMOVED {
			// empty
			currentNodeState.Devices.Empty()
		} else {
			if !currentNodeState.Devices.Equal(verifiedNodeState.Devices) {
				// use verified devices
				currentNodeState.Devices = verifiedNodeState.Devices
				changed = true
			}
		}
	}

	if changed {
		this.OnChanged()
	}

	return
}

// Update ring and other global vars
func (this *DomainMap) OnChanged() (changedNodes *StringSet) {
	MONITOR.SetVariable("M:GOS:Publish", 1)

	// get hashring and device mapping with new node states
	newRing, deviceMapping := newRingFromDomainMap()

	// get changed ticks
	changedNodes = GetChangedNodes(DOMAIN_RING, newRing)

	// update global vars
	DOMAIN_RING = newRing
	DEVICE_MAPPING = deviceMapping

	// update REBALANCE_CACHE
	REBALANCE_CACHE.Update()

	// log
	logger.Debug("Update DomainMap: %s", this)

	return
}

// Admin join node, and can only applied to local node
func (this *DomainMap) AdminJoin() (err error) {
	this.LocalNodeState().AdminJoin()

	changedNodes := this.OnChanged()

	// notify affected nodes to probe this node to update domain map
	for nodeIp := range changedNodes.Data {
		go requestGossipNotify(nodeIp)
	}
	return
}

// Admin remove node
func (this *DomainMap) AdminRemove(nodeIpRemote string) (err error) {
	nodeState, ok := this.Get(nodeIpRemote)
	if !ok {
		return
	}

	nodeState.AdminRemove()

	changedNodes := this.OnChanged()

	// notify affected nodes to probe this node to update domain map
	for nodeIp := range changedNodes.Data {
		go requestGossipNotify(nodeIp)
	}
	return

}

// Check if local node removed
func (this *DomainMap) LocalNodeState() (localNodeState *NodeState) {
	localNodeState, ok := this.Get(NODE_ADDRESS)
	if !ok {
		// should never be here
		localNodeState = NewNodeState()
		localNodeState.StartTime = time.Now()
		this.Set(NODE_ADDRESS, localNodeState)
	}
	return localNodeState
}

// Check if local node removed
func (this *DomainMap) IsLocalNodeRemoved() bool {
	return this.LocalNodeState().Status() == NODE_STATUS_REMOVED
}

// Check if node up, local node is handled as remote node
func (this *DomainMap) IsNodeUp(nodeIp string) bool {
	nodeState, ok := this.Get(nodeIp)
	if !ok {
		return false
	}
	return nodeState.Status() == NODE_STATUS_UP
}

// Check if node in domain
func (this *DomainMap) IsInDomain(nodeIp string) bool {
	nodeState, ok := this.Get(nodeIp)
	if !ok {
		return false
	}
	return nodeState.Status() != NODE_STATUS_REMOVED
}

// Check if node out for rebalance
func (this *DomainMap) IsNodeRebalanceOut(nodeIp string) bool {
	nodeState, ok := this.Get(nodeIp)
	if !ok {
		// not exist
		return true
	}
	if nodeState.Status() == NODE_STATUS_REMOVED {
		// removed
		return true
	}
	if nodeState.Status() == NODE_STATUS_UP {
		// up
		return false
	}
	// down
	if time.Since(nodeState.DownTime) > REBALANCE_OUT_TIME {
		// out
		return true
	}
	return false
}

//// Only rebalance need version, calc it in real time.
//func (this DomainMap) Version() string {
//    hash := sha1.New()

//    // sort nodes
//    nodes := make([]string, 0)
//    for nodeIp := range this.Data {
//        nodes = append(nodes, nodeIp)
//    }
//    sort.Strings(nodes)
//    for _, nodeIp := range nodes {
//        nodeState, ok := this.Get(nodeIp)
//        if !ok {
//            continue
//        }
//        devices := make([]string, 0)
//        for device := range nodeState.Devices {
//            devices = append(devices, device)
//        }
//        sort.Strings(devices)
//        for _, device := range devices {
//            hash.Write([]byte(device))
//        }
//    }

//    return HashBytesToHex(hash.Sum(nil))
//}
