package swarm

import (
	"crypto/sha1"
)

// constants
const (
	// version
	SWARM_VERSION = "0.1"

	// server
	// to fine controll for SA, split each server listen to different ports
	OSS_PORT       = "8081"
	BLOCK_PORT     = "8082"
	GOSSIP_PORT    = "8083"
	REBALANCE_PORT = "8086"
	MONITOR_PORT   = "8087"
	ADMIN_PORT     = "8088"

	// broadcast
	BROADCAST_REQ_PORT  = 8084
	BROADCAST_RESP_PORT = 8085

	// urls
	URL_SWARM_WRITE_OSS = "/swarm/oss/write"
	URL_SWARM_READ_OSS  = "/swarm/oss/read"
	URL_SWARM_QUERY_OSS = "/swarm/oss/query"

	URL_SWARM_REPLICA_BLOCK = "/swarm/block/replica"
	URL_SWARM_WRITE_BLOCK   = "/swarm/block/write"
	URL_SWARM_READ_BLOCK    = "/swarm/block/read"
	URL_SWARM_DEL_BLOCK     = "/swarm/block/del"
	URL_SWARM_BLOCK_STATUS  = "/swarm/block/status"
	URL_SWARM_BLOCK_EXIST   = "/swarm/block/exist"

	URL_SWARM_GOSSIP_PROBE  = "/swarm/gossip/probe"
	URL_SWARM_GOSSIP_VERIFY = "/swarm/gossip/verify"
	URL_SWARM_GOSSIP_NOTIFY = "/swarm/gossip/notify"

	URL_SWARM_REBALANCE_CACHE = "/swarm/rebalance/cache"
	URL_SWARM_REBALANCE_PUSH  = "/swarm/rebalance/push"
	URL_SWARM_REBALANCE_QUERY = "/swarm/rebalance/query"

	URL_SWARM_MONITOR_VERSION  = "/swarm/monitor/version"
	URL_SWARM_MONITOR_QUERY    = "/swarm/monitor/query"
	URL_SWARM_MONITOR_ALL      = "/swarm/monitor/all"
	URL_SWARM_MONITOR_SYSINFO  = "/swarm/monitor/sysinfo"
	URL_SWARM_MONITOR_VARIABLE = "/swarm/monitor/variable"
	URL_SWARM_MONITOR_ALARM    = "/swarm/monitor/alarm"

	URL_SWARM_ADMIN_REBALANCE      = "/swarm/admin/rebalance"
	URL_SWARM_ADMIN_OFFLINE_DEVICE = "/swarm/admin/offline_device"
	URL_SWARM_ADMIN_ONLINE_DEVICE  = "/swarm/admin/online_device"
	URL_SWARM_ADMIN_REMOVE_DEVICE  = "/swarm/admin/remove_device"
	URL_SWARM_ADMIN_DEL_ALARM      = "/swarm/admin/del_alarm"
	URL_SWARM_ADMIN_JOIN_NODE      = "/swarm/admin/join_node"
	URL_SWARM_ADMIN_REMOVE_NODE    = "/swarm/admin/remove_node"

	// environ
	BLOCKS_DIR    = "blocks"
	DEVICES_DIR   = "devices"
	LOGS_DIR      = "logs"
	CONF_DIR      = "conf"
	UUID_FILE     = "uuid"
	DEVICE_PREFIX = "device"

	DOMAIN_CONF_FILE   = "domain.ini"
	NODE_CONF_FILE     = "node.ini"
	NODE_SETTINGS_FILE = "settings.json"

	// block
	BLOCK_TYPE_BLOCK  = "b"
	BLOCK_TYPE_INDEX  = "i"
	BLOCK_TYPE_OBJECT = "o"

	// header
	HEADER_TYPE_BLOCK  = int8(1) // chunk of object
	HEADER_TYPE_INDEX  = int8(2) // chunk index
	HEADER_TYPE_OBJECT = int8(3) // single object

	HEADER_STATE_NEW = int8(1) // normal
	HEADER_STATE_DEL = int8(2) // deleted

	HEADER_SIZE = 44

	HEADER_VERSION = "SWv1"
	HEADER_TAILER  = "SW"

	HASH_HEX_SIZE         = sha1.Size * 2 // sha1 string
	PREFETCH_QUEUQ_LENGTH = int32(2)

	// general status
	STATUS_OK    = 1
	STATUS_ERROR = 2

	// proxied
	STATUS_NOT_PROXIED = "0"
	STATUS_PROXIED     = "1"

	// rebalance
	DEVICE_ID_ALL = "ffffffffffffffffffffffffffffffff"

	// reliable
	ACTION_WRITE = "write"
	ACTION_READ  = "read"
	ACTION_DEL   = "del"

	// gossip
	NODE_STATUS_UP      = "up"
	NODE_STATUS_REMOVED = "removed"
	NODE_STATUS_DOWN    = "down"

	DEVICE_STATUS_ONLINE  = "online"
	DEVICE_STATUS_OFFLINE = "offline"
	DEVICE_STATUS_REMOVED = "removed"

	// admin
	ADMIN_REBALANCE_DUPLICATED = "duplicated rebalance from same node"
	ADMIN_REBALANCE_EXCEED     = "exceed max repair process"

	// device
	DIR_MOD  = 0700
	FILE_MOD = 0600
)

const (
	_ = iota
	ALARM_DUPLICATE_DEVICE
	ALARM_CLOCK_DRIFT
	ALARM_MISMATCH_BLOCKHASH
	ALARM_DEVICE_IOERROR
	ALARM_BLOCKHASHES
	ALARM_SYSCALL
	ALARM_INCOMPATIBLE_VERSION
	ALARM_REBALANCE_CACHE_TIMEOUT
	ALARM_NODE_OUT
)

const (
	_ = iota
	ERROR_INVALID_SCORESEG
)

const MaxInt = int(^uint(0) >> 1)
