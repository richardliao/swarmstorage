package swarm

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"github.com/richardliao/swarm/third_party/code.google.com/p/goconf/conf"
	"path"
	"sort"
	"time"
)

var (
	// init in swarmd
	NODE_BASE_PATH string

	// init in init
	AVAILABLE_BLOCK_TYPES []string
)

func init() {
	AVAILABLE_BLOCK_TYPES = []string{BLOCK_TYPE_BLOCK, BLOCK_TYPE_INDEX}
}

//------------------------------------------------------------------------------
// Domain config
//------------------------------------------------------------------------------

// Domain global vars
var (
	// domain config sig
	DOMAIN_CONFIG_SIG string

	// broadcast
	MULTICAST_ADDR string = "224.0.0.254"
	// block
	BLOCK_SIZE int = 4194304 // 4<<20
	// consistent hash
	DOMAIN_RING_SPOTS  int = 4
	DEVICE_WEIGHT_UNIT float64 = 943718400 // 0.9TB
	// replica
	REPLICA     int = 3
	MIN_REPLICA int = 2
	// timeout
	WRITE_BLOCK_TIMEOUT   time.Duration = 10 * time.Second
	REPLICA_BLOCK_TIMEOUT time.Duration = 10 * time.Second
	GATHER_BLOCK_TIMEOUT  time.Duration = 4 * time.Second
	READ_BLOCK_TIMEOUT    time.Duration = 2 * time.Second
	DEL_BLOCK_TIMEOUT     time.Duration = 1 * time.Second
	DETECT_BLOCK_TIMEOUT  time.Duration = 1 * time.Second
	QUERY_BLOCK_TIMEOUT   time.Duration = 1 * time.Second
	GOSSIP_TIMEOUT        time.Duration = 10 * time.Second
	// block meta
	BLOCK_META_SYNC_INTERVAL time.Duration = 10 * time.Second
	// sweep
	SWEEP_INTERVAL time.Duration = 86400 * time.Second
	// rebalance
	REBALANCE_INTERVAL            time.Duration = 10 * time.Second
	GC_INTERVAL                   time.Duration = 1 * time.Second
	MAX_CONCURRENT_REPAIR         int = 4
	MAX_REBALANCE_IDLE            time.Duration = 600 * time.Second
	MAX_CONCURRENT_REBALANCE      int = 4
	MAX_CONCURRENT_GC             int = 4
	QUERY_REBALANCE_CACHE_TIMEOUT time.Duration = 120 * time.Second
	REBALANCE_OUT_TIME            time.Duration = 60 * time.Second
	// reliable
	ACCESS_INTERMITTENT time.Duration = 10 * time.Second
	// gossip
	MAX_NODE_TIME_DIFF         time.Duration = 10 * time.Second
	POLL_NEIGHBOR_INTERVAL     time.Duration = 2 * time.Second
	NODE_CLEANUP_TIME          time.Duration = 400 * time.Second
	NODE_OUT_TIME              time.Duration = 30 * time.Second
	GOSSIP_NOTIFY_QUEUE_LENGTH int = 5
	// throttle
	CPU_BUSY_THROTLLE  uint64 = 90
	DISK_BUSY_THROTLLE uint64 = 50
	// device manager
	READ_QUEUE_LENGTH  int = 128
	WRITE_QUEUE_LENGTH int = 32
)

func getDomainConfig() (m map[string]interface{}) {
    m = make(map[string]interface{})
    m["MULTICAST_ADDR"] = MULTICAST_ADDR
    m["BLOCK_SIZE"] = BLOCK_SIZE
    m["DOMAIN_RING_SPOTS"] = DOMAIN_RING_SPOTS
    m["DEVICE_WEIGHT_UNIT"] = DEVICE_WEIGHT_UNIT
    m["REPLICA"] = REPLICA
    m["MIN_REPLICA"] = MIN_REPLICA
    m["WRITE_BLOCK_TIMEOUT"] = WRITE_BLOCK_TIMEOUT
    m["REPLICA_BLOCK_TIMEOUT"] = REPLICA_BLOCK_TIMEOUT
    m["GATHER_BLOCK_TIMEOUT"] = GATHER_BLOCK_TIMEOUT
    m["READ_BLOCK_TIMEOUT"] = READ_BLOCK_TIMEOUT
    m["DEL_BLOCK_TIMEOUT"] = DEL_BLOCK_TIMEOUT
    m["DETECT_BLOCK_TIMEOUT"] = DETECT_BLOCK_TIMEOUT
    m["QUERY_BLOCK_TIMEOUT"] = QUERY_BLOCK_TIMEOUT
    m["GOSSIP_TIMEOUT"] = GOSSIP_TIMEOUT
    m["BLOCK_META_SYNC_INTERVAL"] = BLOCK_META_SYNC_INTERVAL
    m["SWEEP_INTERVAL"] = SWEEP_INTERVAL
    m["REBALANCE_INTERVAL"] = REBALANCE_INTERVAL
    m["GC_INTERVAL"] = GC_INTERVAL
    m["MAX_CONCURRENT_REPAIR"] = MAX_CONCURRENT_REPAIR
    m["MAX_REBALANCE_IDLE"] = MAX_REBALANCE_IDLE
    m["MAX_CONCURRENT_REBALANCE"] = MAX_CONCURRENT_REBALANCE
    m["MAX_CONCURRENT_GC"] = MAX_CONCURRENT_GC
    m["QUERY_REBALANCE_CACHE_TIMEOUT"] = QUERY_REBALANCE_CACHE_TIMEOUT
    m["REBALANCE_OUT_TIME"] = REBALANCE_OUT_TIME
    m["ACCESS_INTERMITTENT"] = ACCESS_INTERMITTENT
    m["MAX_NODE_TIME_DIFF"] = MAX_NODE_TIME_DIFF
    m["POLL_NEIGHBOR_INTERVAL"] = POLL_NEIGHBOR_INTERVAL
    m["NODE_CLEANUP_TIME"] = NODE_CLEANUP_TIME
    m["NODE_OUT_TIME"] = NODE_OUT_TIME
    m["GOSSIP_NOTIFY_QUEUE_LENGTH"] = GOSSIP_NOTIFY_QUEUE_LENGTH
    m["CPU_BUSY_THROTLLE"] = CPU_BUSY_THROTLLE
    m["DISK_BUSY_THROTLLE"] = DISK_BUSY_THROTLLE
    m["READ_QUEUE_LENGTH"] = READ_QUEUE_LENGTH
    m["WRITE_QUEUE_LENGTH"] = WRITE_QUEUE_LENGTH
    return
}

func getDomainConfigSig() (domain_config_sig string) {
    m := getDomainConfig()

	// get options
    options := make([]string, 0)
    for option := range m {
        options = append(options, option)
    }

	// sort options
	sort.Strings(options)

	// calc hash
	hash := sha1.New()
	for _, option := range options {
		if value, ok := m[option]; ok {
			hash.Write([]byte(fmt.Sprintf("%s", value)))
		}
	}

	// update sig
	domain_config_sig = HashBytesToHex(hash.Sum(nil))
	return
}

// Load domain config
func LoadDomainConfig() (err error) {
    // calc default sig
    DOMAIN_CONFIG_SIG = getDomainConfigSig()

	// parse ini file
	confPath := path.Join(NODE_BASE_PATH, CONF_DIR, DOMAIN_CONF_FILE)
	c, err := conf.ReadConfigFile(confPath)
	if err != nil {
	    // file not exist, use default values
		return nil
	}

	// set global var
	if v, err := c.GetString("default", "MULTICAST_ADDR"); err == nil {
		MULTICAST_ADDR = v
	}
	if v, err := c.GetInt("default", "BLOCK_SIZE"); err == nil {
		BLOCK_SIZE = v
	}
	if v, err := c.GetInt("default", "DOMAIN_RING_SPOTS"); err == nil {
		DOMAIN_RING_SPOTS = v
	}
	if v, err := c.GetFloat64("default", "DEVICE_WEIGHT_UNIT"); err == nil {
		DEVICE_WEIGHT_UNIT = v
	}
	if v, err := c.GetInt("default", "REPLICA"); err == nil {
		REPLICA = v
	}
	if v, err := c.GetInt("default", "MIN_REPLICA"); err == nil {
		MIN_REPLICA = v
	}
	if v, err := c.GetSecond("default", "WRITE_BLOCK_TIMEOUT"); err == nil {
		WRITE_BLOCK_TIMEOUT = v
	}
	if v, err := c.GetSecond("default", "REPLICA_BLOCK_TIMEOUT"); err == nil {
		REPLICA_BLOCK_TIMEOUT = v
	}
	if v, err := c.GetSecond("default", "GATHER_BLOCK_TIMEOUT"); err == nil {
		GATHER_BLOCK_TIMEOUT = v
	}
	if v, err := c.GetSecond("default", "READ_BLOCK_TIMEOUT"); err == nil {
		READ_BLOCK_TIMEOUT = v
	}
	if v, err := c.GetSecond("default", "DEL_BLOCK_TIMEOUT"); err == nil {
		DEL_BLOCK_TIMEOUT = v
	}
	if v, err := c.GetSecond("default", "DETECT_BLOCK_TIMEOUT"); err == nil {
		DETECT_BLOCK_TIMEOUT = v
	}
	if v, err := c.GetSecond("default", "QUERY_BLOCK_TIMEOUT"); err == nil {
		QUERY_BLOCK_TIMEOUT = v
	}
	if v, err := c.GetSecond("default", "GOSSIP_TIMEOUT"); err == nil {
		GOSSIP_TIMEOUT = v
	}
	if v, err := c.GetSecond("default", "BLOCK_META_SYNC_INTERVAL"); err == nil {
		BLOCK_META_SYNC_INTERVAL = v
	}
	if v, err := c.GetSecond("default", "SWEEP_INTERVAL"); err == nil {
		SWEEP_INTERVAL = v
	}
	if v, err := c.GetSecond("default", "REBALANCE_INTERVAL"); err == nil {
		REBALANCE_INTERVAL = v
	}
	if v, err := c.GetSecond("default", "GC_INTERVAL"); err == nil {
		GC_INTERVAL = v
	}
	if v, err := c.GetInt("default", "MAX_CONCURRENT_REPAIR"); err == nil {
		MAX_CONCURRENT_REPAIR = v
	}
	if v, err := c.GetSecond("default", "MAX_REBALANCE_IDLE"); err == nil {
		MAX_REBALANCE_IDLE = v
	}
	if v, err := c.GetInt("default", "MAX_CONCURRENT_REBALANCE"); err == nil {
		MAX_CONCURRENT_REBALANCE = v
	}
	if v, err := c.GetInt("default", "MAX_CONCURRENT_GC"); err == nil {
		MAX_CONCURRENT_GC = v
	}
	if v, err := c.GetSecond("default", "QUERY_REBALANCE_CACHE_TIMEOUT"); err == nil {
		QUERY_REBALANCE_CACHE_TIMEOUT = v
	}
	if v, err := c.GetSecond("default", "REBALANCE_OUT_TIME"); err == nil {
		REBALANCE_OUT_TIME = v
	}
	if v, err := c.GetSecond("default", "ACCESS_INTERMITTENT"); err == nil {
		ACCESS_INTERMITTENT = v
	}
	if v, err := c.GetSecond("default", "MAX_NODE_TIME_DIFF"); err == nil {
		MAX_NODE_TIME_DIFF = v
	}
	if v, err := c.GetSecond("default", "POLL_NEIGHBOR_INTERVAL"); err == nil {
		POLL_NEIGHBOR_INTERVAL = v
	}
	if v, err := c.GetSecond("default", "NODE_CLEANUP_TIME"); err == nil {
		NODE_CLEANUP_TIME = v
	}
	if v, err := c.GetSecond("default", "NODE_OUT_TIME"); err == nil {
		NODE_OUT_TIME = v
	}
	if v, err := c.GetInt("default", "GOSSIP_NOTIFY_QUEUE_LENGTH"); err == nil {
		GOSSIP_NOTIFY_QUEUE_LENGTH = v
	}
	if v, err := c.GetUint64("default", "CPU_BUSY_THROTLLE"); err == nil {
		CPU_BUSY_THROTLLE = v
	}
	if v, err := c.GetUint64("default", "DISK_BUSY_THROTLLE"); err == nil {
		DISK_BUSY_THROTLLE = v
	}
	if v, err := c.GetInt("default", "READ_QUEUE_LENGTH"); err == nil {
		READ_QUEUE_LENGTH = v
	}
	if v, err := c.GetInt("default", "WRITE_QUEUE_LENGTH"); err == nil {
		WRITE_QUEUE_LENGTH = v
	}

	// update DOMAIN_CONFIG_SIG
	DOMAIN_CONFIG_SIG = getDomainConfigSig()
	return
}

//------------------------------------------------------------------------------
// Node config
//------------------------------------------------------------------------------

// Node global vars
var (
	NODE_ADDRESS   string
	OSS_ADDRESS    string
	LOG_FILE       string
	LOG_LEVEL      int
	DOMAIN_ID      string
	DOMAIN_PUB_KEY *rsa.PublicKey
	NODE_CERT      *NodeCert
	NODE_SIG       []byte
)

// Load node config
func LoadNodeConfig() (err error) {
	// parse ini file
	confPath := path.Join(NODE_BASE_PATH, CONF_DIR, NODE_CONF_FILE)
	c, err := conf.ReadConfigFile(confPath)
	if err != nil {
		return
	}

	// set global var
	NODE_ADDRESS, err = c.GetString("default", "NODE_ADDRESS")
	if err != nil {
		return
	}

	OSS_ADDRESS, err = c.GetString("default", "OSS_ADDRESS")
	if err != nil {
		return
	}

	LOG_FILE, err = c.GetString("default", "LOG_FILE")
	if err != nil {
		return
	}
	if LOG_FILE[0:1] != "/" {
		// convert to abs path
		LOG_FILE = path.Join(NODE_BASE_PATH, LOG_FILE)
	}

	level, err := c.GetString("default", "LOG_LEVEL")
	if err != nil {
		return
	}
	switch level {
	case "FATAL":
		LOG_LEVEL = FATAL
	case "CRITICAL":
		LOG_LEVEL = CRITICAL
	case "ERROR":
		LOG_LEVEL = ERROR
	case "WARN":
		LOG_LEVEL = WARN
	case "INFO":
		LOG_LEVEL = INFO
	case "DEBUG":
		LOG_LEVEL = DEBUG
	default:
		err = fmt.Errorf("Invalid log level %s", level)
		return
	}

	// cert
	DOMAIN_ID, err = c.GetString("default", "DOMAIN_ID")
	if err != nil {
		return
	}

	encoded, err := c.GetString("default", "DOMAIN_PUB_KEY")
	if err != nil {
		return
	}
	DOMAIN_PUB_KEY, err = loadDomainPubKey([]byte(encoded))
	if err != nil {
		return
	}

	encoded, err = c.GetString("default", "NODE_SIG")
	if err != nil {
		return
	}
	NODE_SIG, err = loadNodeSig(encoded)
	if err != nil {
		return
	}

	NODE_CERT = &NodeCert{
		DomainId: DOMAIN_ID,
		NodeId:   NODE_ADDRESS,
	}

	// verify NODE_SIG
	s, err := json.Marshal(NODE_CERT)
	if err != nil {
		return
	}
	if err = rsa.VerifyPKCS1v15(DOMAIN_PUB_KEY, crypto.SHA1, Sha1Bytes(s), NODE_SIG); err != nil {
		return
	}

	return nil
}
