package swarm

import (
	"time"
)

/*
Maintain nodes access status.
*/
type NodeReliable struct {
	Success time.Time
	Fail    time.Time
}

// Global
var NODE_RELIABILITIES map[string]*NodeReliable

func init() {
	NODE_RELIABILITIES = make(map[string]*NodeReliable)
}

// Record node readwrite success/failure
func RecordAccessResult(nodeIp string, action string, success bool) {
	nodeReliable, ok := NODE_RELIABILITIES[nodeIp]
	if !ok {
		nodeReliable = &NodeReliable{}
		NODE_RELIABILITIES[nodeIp] = nodeReliable
	}

	if success {
		nodeReliable.Success = time.Now()
	} else {
		nodeReliable.Fail = time.Now()
	}
}

// Can access node for read/write block
// Read/write failure will make a node unaccessable for 10s.
func CanAccess(nodeIp string) bool {
	if !DOMAIN_MAP.IsNodeUp(nodeIp) {
		return false
	}

	nodeReliable, ok := NODE_RELIABILITIES[nodeIp]
	if !ok {
		// no record, assume node is accessable
		return true
	}

	if nodeReliable.Success.After(nodeReliable.Fail) {
		// last access success
		return true
	}

	if time.Since(nodeReliable.Fail) < ACCESS_INTERMITTENT {
		// not accessable for 10s
		return false
	}
	return true
}
