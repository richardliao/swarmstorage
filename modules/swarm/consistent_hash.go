package swarm

import (
	"crypto/sha1"
	"fmt"
	"sort"
	"strconv"
)

//------------------------------------------------------------------------------
// hashRing
//------------------------------------------------------------------------------

// Represent a logical group of ring spots.
type Tick struct {
	Name  string
	Score uint32
}

func NewTick(name string, score uint32) Tick {
	return Tick{
		Name:  name,
		Score: score,
	}
}

type tickArray []Tick

func (p tickArray) Len() int           { return len(p) }
func (p tickArray) Less(i, j int) bool { return p[i].Score < p[j].Score }
func (p tickArray) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p tickArray) Sort()              { sort.Sort(p) }

type hashRing struct {
	defaultSpots int
	ticks        tickArray
	length       int
}

// n: number of spots a node per weight owns
func NewRing(n int) *hashRing {
	h := new(hashRing)
	h.defaultSpots = n
	return h
}

// Adds a new node to a hash ring
// Don't confuse with a physical node in domain network.
// Actually, it is a device in our circumstance.
// name: id of the device
// weight: multiplier for default number of ticks (useful when one cache node has more resources, like RAM, than another)
// TODO: check device id to avoid score collision
func (this *hashRing) AddNode(name string, weight int) {
	scores := GetNodeScores(name, this.defaultSpots, weight)

	for _, score := range scores {
		tick := NewTick(name, score)
		this.ticks = append(this.ticks, tick)
	}
}

// Finish add nodes, should be called after modify the ring.
func (this *hashRing) Finish() {
	this.ticks.Sort()
	this.length = len(this.ticks)
}

// Get next tick
func (this *hashRing) GetNextTick(score uint32) (tick Tick, err error) {
	// get index match score
	i := sort.Search(this.length, func(i int) bool { return this.ticks[i].Score > score })
	if i == this.length {
		i = 0
	}
	i--
	if i < 0 {
		i = this.length - 1
	}

	// get index of next tick
	iNext := i + 1
	if iNext >= len(this.ticks) {
		iNext = 0
	}

	if iNext >= this.length || iNext < 0 {
		err = fmt.Errorf("Invalid hash ring to GetNextTick, length %d, index %d", this.length, iNext)
		return
	}

	// get tick
	tick = this.ticks[iNext]
	return
}

// Get prev tick
func (this *hashRing) GetPrevTick(score uint32) (tick Tick, err error) {
	// get index match score
	i := sort.Search(this.length, func(i int) bool { return this.ticks[i].Score > score })
	if i == this.length {
		i = 0
	}
	i--
	if i < 0 {
		i = this.length - 1
	}

	// get index of prev tick
	iPrev := i - 1
	if iPrev < 0 {
		iPrev = len(this.ticks) - 1
	}

	if iPrev >= this.length || iPrev < 0 {
		err = fmt.Errorf("Invalid hash ring to GetPrevTick, length %d, index %d", this.length, iPrev)
		return
	}

	// get tick
	tick = this.ticks[iPrev]
	return
}

// Get primary device
func (this *hashRing) GetPrimaryDevice(blockhash string) (deviceId string, err error) {
	tick, err := this.GetPrimaryTick(blockhash)
	if err != nil {
		return
	}
	return tick.Name, nil
}

// Get the primary tick
func (this *hashRing) GetPrimaryTick(blockhash string) (tick Tick, err error) {
	score, err := GetBlockScore(blockhash)
	if err != nil {
		return
	}
	//	fmt.Printf("score %#v\n", score)

	tick, err = this.GetPrimaryTickByScore(score)
	return
}

// Get the primary tick
func (this *hashRing) GetPrimaryTickByScore(score uint32) (tick Tick, err error) {
	i := sort.Search(this.length, func(i int) bool { return this.ticks[i].Score > score })
	if i == this.length {
		// not found
		i = 0
	}
	i--
	if i < 0 {
		i = this.length - 1
	}

	if i >= this.length || i < 0 {
		err = fmt.Errorf("Invalid hash ring, length %d, index %d", this.length, i)
		return
	}

	return this.ticks[i], nil
}

func (this *hashRing) Contains(tick1 Tick) bool {
	for _, tick := range this.ticks {
		if tick == tick1 {
			return true
		}
	}
	return false
}

// Get nodes from hashring
// Generally, reblanace include down, read/write exclude down
func (this *hashRing) GetReplicaNodes(blockhash string, replica int, mustUp bool) (nodes map[string]string, err error) {
	initTick, err := this.GetPrimaryTick(blockhash)
	if err != nil {
		return
	}
	_, nodes, err = this.GetReplicaTicks(0, initTick, replica, mustUp, false)
	if err != nil {
		return
	}

	return nodes, nil
}

// Get replica nodes
// priority: 0 primary, 1, 2
// nodes: {nodeIp: deviceId}
func (this *hashRing) GetReplicaTicks(initPriority int, initTick Tick, replica int,
	mustUp bool, includeOut bool) (ticks map[Tick]interface{}, nodes map[string]string, err error) {
	if initPriority >= replica {
		err = fmt.Errorf("Invalid param to getReplicaTicks, initPriority %d, replica %d", initPriority, replica)
		return
	}
	if len(DOMAIN_MAP.Data) < replica {
		err = fmt.Errorf("Not enough ring to replica %d", replica)
		return
	}

	// init
	ticks = make(map[Tick]interface{})
	nodes = make(map[string]string)

	// save initTick
	nodeIp, ok := DEVICE_MAPPING[initTick.Name]
	if !ok {
		err = fmt.Errorf("Invalid DEVICE_MAPPING to get device %s", initTick.Name)
		return
	}
	if !mustUp {
		if includeOut || !DOMAIN_MAP.IsNodeRebalanceOut(nodeIp) {
			ticks[initTick] = nil
			nodes[nodeIp] = initTick.Name
		}
	} else {
		if CanAccess(nodeIp) {
			ticks[initTick] = nil
			nodes[nodeIp] = initTick.Name
		}
	}

	// search until primay
	tick := initTick
	for priority := initPriority - 1; priority >= 0; {
		tick, err = this.GetNextTick(tick.Score)
		if err != nil {
			return
		}
		if tick.Score == initTick.Score {
			// loop over
			err = fmt.Errorf("Not enough ring to replica %d", replica)
			return
		}
		nodeIp, ok = DEVICE_MAPPING[tick.Name]
		if !ok {
			// fatal error
			err = fmt.Errorf("Invalid DEVICE_MAPPING to get %s", tick.Name)
			return
		}
		if _, ok := nodes[nodeIp]; ok {
			// duplicate node
			continue
		}
		// found one
		if !mustUp {
			if includeOut || !DOMAIN_MAP.IsNodeRebalanceOut(nodeIp) {
				ticks[tick] = nil
				nodes[nodeIp] = tick.Name
			}
		} else {
			if CanAccess(nodeIp) {
				ticks[tick] = nil
				nodes[nodeIp] = tick.Name
			}
		}
		// next
		priority--
	}

	// search remains
	tick = initTick
	for priority := initPriority + 1; len(nodes) < replica; {
		tick, err = DOMAIN_RING.GetPrevTick(tick.Score)
		if err != nil {
			return
		}
		if tick.Score == initTick.Score {
			// loop over
			err = fmt.Errorf("Not enough ring to replica %d", replica)
			return
		}
		nodeIp, ok = DEVICE_MAPPING[tick.Name]
		if !ok {
			// fatal error
			err = fmt.Errorf("Invalid DEVICE_MAPPING to get %s", tick.Name)
			return
		}
		if _, ok := nodes[nodeIp]; ok {
			// duplicate node
			continue
		}
		// found one
		if !mustUp {
			if includeOut || !DOMAIN_MAP.IsNodeRebalanceOut(nodeIp) {
				ticks[tick] = nil
				nodes[nodeIp] = tick.Name
			}
		} else {
			if CanAccess(nodeIp) {
				ticks[tick] = nil
				nodes[nodeIp] = tick.Name
			}
		}
		// next
		priority++
	}

	// unnecessary check, but in case anything wrong
	if len(nodes) != replica || len(ticks) != replica {
		err = fmt.Errorf("Not enough ring to replica %d", replica)
		return
	}

	return
}

//------------------------------------------------------------------------------
// helper
//------------------------------------------------------------------------------

// Get bytes score
// Choose the last 4 bytes for score, as BigEndian
func GetBytesScore(hashBytes []byte) (score uint32) {
	return uint32(hashBytes[16])<<24 | uint32(hashBytes[17])<<16 | uint32(hashBytes[18])<<8 | uint32(hashBytes[19])
}

// Get node scores
func GetNodeScores(name string, defaultSpots int, weight int) (scores []uint32) {
	spots := defaultSpots * weight
	hash := sha1.New()
	for i := 1; i <= spots; i++ {
		hash.Write([]byte(name + ":" + strconv.Itoa(i)))
		hashBytes := hash.Sum(nil)

		scores = append(scores, GetBytesScore(hashBytes))

		// reuse hash object for performance
		hash.Reset()
	}
	return
}

// Get blockhash score
func GetBlockScore(blockhash string) (score uint32, err error) {
	hashBytes, err := HashHexToBytes(blockhash, HASH_HEX_SIZE)
	if err != nil {
		return score, err
	}

	score = GetBytesScore(hashBytes)
	return
}

//------------------------------------------------------------------------------
// domain ring
//------------------------------------------------------------------------------

// Get new ring from current domain map
func newRingFromDomainMap() (ring *hashRing, deviceMapping map[string]string) {
	// init
	ring = NewRing(DOMAIN_RING_SPOTS)
	deviceMapping = make(map[string]string)

	for nodeIp, nodeState := range DOMAIN_MAP.Data {
		// skip down node
		if nodeState.Status() == NODE_STATUS_REMOVED {
			continue
		}
		// add devices
		for deviceId, weight := range nodeState.Devices.Data {
			// add to device mapping
			if _, ok := deviceMapping[deviceId]; ok {
				ALARM_MANAGER.Add(ALARM_DUPLICATE_DEVICE, fmt.Errorf(""), "Duplicate device %s from node %s", deviceId, nodeIp)
				continue
			}
			deviceMapping[deviceId] = nodeIp
			// add to ring
			ring.AddNode(deviceId, *weight)
		}
	}
	ring.Finish()
	return
}

// Get changed ticks
// It is ok to get result nodes more than actual nodes.
func GetChangedNodes(oldRing *hashRing, newRing *hashRing) (changedNodes *StringSet) {
	changedNodes = NewStringSet()

	// get nodes afftected by deleting ticks
	for _, tick := range oldRing.ticks {
		if !newRing.Contains(tick) {
			// new primary tick
			newTick, err := oldRing.GetPrevTick(tick.Score)
			if err != nil {
				continue
			}
			// new replicas
			_, nodes, err := newRing.GetReplicaTicks(0, newTick, REPLICA, false, true)
			if err != nil {
				continue
			}
			for nodeIp := range nodes {
				if nodeIp != NODE_ADDRESS {
					changedNodes.Add(nodeIp)
				}
			}
		}
	}

	// get nodes afftected by adding ticks
	for _, tick := range newRing.ticks {
		if !oldRing.Contains(tick) {
			// old primary tick
			oldTick, err := newRing.GetPrevTick(tick.Score)
			if err != nil {
				continue
			}
			// old replicas
			_, nodes, err := oldRing.GetReplicaTicks(0, oldTick, REPLICA, false, true)
			if err != nil {
				continue
			}
			for nodeIp := range nodes {
				if nodeIp != NODE_ADDRESS {
					changedNodes.Add(nodeIp)
				}
			}
		}
	}

	return
}
