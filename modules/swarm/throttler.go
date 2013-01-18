package swarm

import (
	"time"
)

//------------------------------------------------------------------------------
// Throttler
//------------------------------------------------------------------------------

/*
The purpose of throttle strategy is to coordinate contend for various resources,
and to avoid congestion in critical period of time.

Currently, it coordinate resources of local node, and domain wide in the future.

TODO: setup throttle priority for different actions:
low: sweep
norm: rebalance
high: metacache, gossip
*/
// Wait for resource before do resource critical and low priority task
func throttle(cmd string, deviceId string) {
	switch cmd {
	case "updateLocalDevices", "probeNode":
		// TODO: CPU and all disks critical
		before := time.Now()
		for {
			if time.Now().Sub(before) > 10*time.Second {
				// pass after 10s.
				return
			}
			if MONITOR.GetCpuBusySecond() > CPU_BUSY_THROTLLE {
				time.Sleep(1e9)
			} else {
				return
			}
		}

	case "pushRebalanceMessage", "repairFromNode":
		// TODO: CPU, all disks and remote node critical
		for {
			if MONITOR.GetCpuBusySecond() > CPU_BUSY_THROTLLE {
				time.Sleep(1e9)
			} else {
				return
			}
		}
	case "doPushRebalance":
		// CPU critical
		for {
			if MONITOR.GetCpuBusySecond() > CPU_BUSY_THROTLLE {
				time.Sleep(1e9)
			} else {
				return
			}
		}
	case "updateDeviceMeta", "cleanupMetaCache", "tryRebalanceGC", "sweep":
		// CPU and disk critical
		before := time.Now()
		for {
			if time.Now().Sub(before) > 10*time.Second {
				// pass after 10s.
				return
			}
			if MONITOR.GetCpuBusySecond() > CPU_BUSY_THROTLLE || MONITOR.GetDiskBusySecond(deviceId) > DISK_BUSY_THROTLLE {
				time.Sleep(1e9)
			} else {
				return
			}
		}
	default:
		return
	}
}
