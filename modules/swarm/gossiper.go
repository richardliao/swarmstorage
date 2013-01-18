package swarm

import (
	"fmt"
	"math/rand"
	"time"
)

/*
How does ring turbulence effections be limited:

A primary tick need access 2 remote ticks, and also has 2 replica ticks of 2
remote ticks. 

Ticks a device stored:
  (1 + 2) * weight * DOMAIN_RING_SPOTS

Each node need to access
  (2 + 2) * weight * DOMAIN_RING_SPOTS * devices 
remote nodes for read/write/rebalance, that is the main data stream for a node.

So, add/remove a device will only affect limited nodes.

If we can maintain nodes stable, the domain map is globally stable, expect for
above limited afftected nodes.

The domain map stable time of correlate nodes (only by disk failure):
    30,000 / ((2 + 2) * weight * DOMAIN_RING_SPOTS * devices) 
    30000  / ((2 + 2) * 3      * 4                 * 24     ) = 26 hours

We need boost domain map convergency, target is 1 hour.

To do so, push gossip to affected nodes when devices changed. 

Global domain version will be different among affected nodes and unaffected 
nodes. So, just verify related ring tick when rebalance.
*/

/*
How gossip handle scale out:

In huge domain, such as contains tens of thousand nodes, random gossip is 
inefficient to handle the effect of nodes status change. Though each node just 
need to contact limited nodes to exchange data, but it is infeasible to 
accelerate gossip speed by probing correlate nodes more frequently than others.

For a fully equipped node, correlate nodes is:
    (2 + 2) * weight * DOMAIN_RING_SPOTS * devices 
    (2 + 2) * 3      * 4                 * 24       = 1152

If the frequency to probe each correlate node is 10 seconds, the period to poll
all correlate nodes is about 3 hours. Although it is limited, but is too slow 
and unacceptable.

The fact is propogating correlate status changes is heavily depend on gossip 
notify of connection exceptions when rebalance and read/write block. 

Gossip poll just assists to update correlate node status. Its main purpose is to 
discover new nodes.

*/

//------------------------------------------------------------------------------
// GossipNotifier
//------------------------------------------------------------------------------

type GossipNotifier chan string

func NewGossipNotifier() GossipNotifier {
	return make(GossipNotifier, GOSSIP_NOTIFY_QUEUE_LENGTH)
}

// Enqueue gossip task, queue is controled to avoid too busy
func (this GossipNotifier) Add(nodeIp string) {
	if !DOMAIN_MAP.IsNodeUp(nodeIp) {
		// skip down node
		return
	}

	if len(this) == GOSSIP_NOTIFY_QUEUE_LENGTH {
		// discard oldest task
		<-this
	}

	// enqueue
	this <- nodeIp
}

//------------------------------------------------------------------------------
// Gossiper
//------------------------------------------------------------------------------

/*
Periodically:

* update local devices;

* explore neighbor nodes: find new nodes, record failed nodes;

* merge remote node states: verify node state and merge

* update local node states;
*/
type Gossiper struct {
	WorkerBase
	watchdog       *Watchdog
	GossipNotifier GossipNotifier
}

// Implement interface Restartable
func (this *Gossiper) Name() string {
	return "Gossiper"
}

// Implement interface Restartable
func (this *Gossiper) Start(r Restartable, e interface{}) {
	defer func() {
		e := recover()
		this.OnPanic(this, this.watchdog, e)
	}()

	logger.Critical("Gossiper started.")

	startIpNum, maskNum := getNeighbors()
	for {
		// monitor local devices change
		devices := LOCAL_DEVICES.Scan()
		LOCAL_DEVICES.Update(devices)
		//logger.Debug("devices: %#v", devices)

		// handle gossip notify queue until empty or exceed limit
		for limit := 0; limit <= GOSSIP_NOTIFY_QUEUE_LENGTH; limit++ {
			select {
			case nodeIp := <-this.GossipNotifier:
				if nodeIp != NODE_ADDRESS {
					requestGossipProbe(nodeIp)
				}
			default:
				// finished
				break
			}
			// pause a while
			time.Sleep(100 * time.Millisecond)
		}

		// poll neighbor
		this.pollNeighbors(startIpNum, maskNum)

		// cleanup
		this.CleanupDomainMap()

		// pause a while
		time.Sleep(randInterval(POLL_NEIGHBOR_INTERVAL))
	}

	logger.Critical("Gossiper stopped.")
}

// Poll neighbors
func (this *Gossiper) pollNeighbors(startIpNum uint32, maskNum uint32) {
	MONITOR.SetVariable("M:GOS:PollNeighbors", 1)

	// select a random node
	offset := uint32(rand.Int31n(int32(maskNum))) + 1
	remoteIpNum := startIpNum + offset
	nodeIp := numToDotted(remoteIpNum)
	if nodeIp == NODE_ADDRESS {
		// skip local node
		return
	}

	if nodeState, ok := DOMAIN_MAP.Get(nodeIp); ok && nodeState.Status() == NODE_STATUS_REMOVED {
		// not gossip removed node
		return
	}

	// probe
	requestGossipProbe(nodeIp)
}

// Cleanup obsolete node states
func (this *Gossiper) CleanupDomainMap() {
	for nodeIp, nodeState := range DOMAIN_MAP.Data {
		if nodeIp == NODE_ADDRESS {
			// not cleanup local node
			continue
		}
		if nodeState.Status() == NODE_STATUS_REMOVED && time.Since(nodeState.RemoveTime) > NODE_CLEANUP_TIME {
			// clean removed node
			DOMAIN_MAP.Delete(nodeIp)
		}
		if nodeState.Status() == NODE_STATUS_DOWN && time.Since(nodeState.DownTime) > NODE_OUT_TIME {
			// alarm out node
			ALARM_MANAGER.Add(ALARM_NODE_OUT, fmt.Errorf(""), "Node %s is out", nodeIp)

			if time.Since(nodeState.DownTime) > REBALANCE_OUT_TIME && !REBALANCE_CACHE.Out.Contains(nodeIp) {
				REBALANCE_CACHE.Out.Add(nodeIp)
				// update rebalance cache, to cache relayed ticks
				REBALANCE_CACHE.Update()
			}
		}
	}
}
