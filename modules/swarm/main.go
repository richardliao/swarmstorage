package swarm

import (
	"fmt"
	"os"
	"time"
)

// Global vars
var (
	logger Logger

	// init in PreStart
	NODE_SETTINGS NodeSettings

	// init in Start
	MONITOR Monitor

	// init in init
	ALARM_MANAGER   AlarmManager
	DEVICE_MANAGER  DeviceManager
	GOSSIP_NOTIFIER GossipNotifier

	// The range expression is evaluated once before beginning the loop.
	// If map entries that have not yet been reached are deleted during 
	// iteration, the corresponding iteration values will not be produced. If 
	// map entries are inserted during iteration, the behavior is 
	// implementation-dependent, but the iteration values for each entry will be 
	// produced at most once.

	// Since map operations are not atomic/thread-safe, reading/writing map 
	// always need be synchronized. 

	// Below global variables use map as underlay data structure. All write 
	// and single read operations are synchronized. The only unsynchronized 
	// operation is itererate operation, which works only when caller can 
	// tolerate incorrect result, or can detect and abandon bad values before 
	// use them.
	LOCAL_DEVICES *LocalDevices
	DOMAIN_MAP    *DomainMap

	// replaced in one action, not synchronized
	DOMAIN_RING    *hashRing
	DEVICE_MAPPING map[string]string // {deviceId: nodeIp}
)

func init() {
	ALARM_MANAGER = NewAlarmManager()
	DEVICE_MANAGER = NewDeviceManager()
	GOSSIP_NOTIFIER = NewGossipNotifier()

	LOCAL_DEVICES = NewLocalDevices()
	DOMAIN_MAP = NewDomainMap()

	DOMAIN_RING = NewRing(DOMAIN_RING_SPOTS)
	DEVICE_MAPPING = make(map[string]string)
}

func PreStart() {
	// load domain and node config to init global vars
	// this must be called before any further functions using these global vars
	if err := LoadDomainConfig(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed load domain config, error: %s.\n", err)
		os.Exit(1)
	}
	if err := LoadNodeConfig(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed load node config, error: %s.\n", err)
		os.Exit(1)
	}

	// load node settings
	NODE_SETTINGS = NewNodeSettings()
	if err := NODE_SETTINGS.Load(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed load node settings, error: %s.\n", err)
		os.Exit(1)
	}
}

// Start swarm
func Start() {
	defer onFatalPanic()

	// post init
	PreStart()

	// init log
	logger = InitLog(LOG_FILE)

	logger.Critical("Swarm started.")

	// add local node in DOMAIN_MAP
	// NODE_ADDRESS is valid after LoadNodeConfig
	localNodeState := NewNodeState()
	localNodeState.StartTime = time.Now()
	DOMAIN_MAP.Set(NODE_ADDRESS, localNodeState)

	// init Watchdog
	w := NewWatchdog()
	go w.Start()

	// init Monitor
	MONITOR = Monitor{
		watchdog:     w,
		ssData:       make(map[string]*SSVariable),
		analysisData: make(map[string]*AnalysisData),
		valueChan:    make(chan *SSValue, 1000),
		commandChan:  make(chan *MonitorCommand),
	}
	go MONITOR.Start(&MONITOR, nil)

	// init MonitorServer
	monitorServer := MonitorServer{
		watchdog: w,
	}
	go monitorServer.Start(&monitorServer, nil)

	// init OSS
	oss := OSS{
		watchdog: w,
	}
	go oss.Start(&oss, nil)

	// init BlockServer
	blockServer := BlockServer{
		watchdog: w,
	}
	go blockServer.Start(&blockServer, nil)

	// init GossipServer
	gossipServer := GossipServer{
		watchdog: w,
	}
	go gossipServer.Start(&gossipServer, nil)

	// init Gossiper
	gossiper := Gossiper{
		watchdog:       w,
		GossipNotifier: GOSSIP_NOTIFIER,
	}
	go gossiper.Start(&gossiper, nil)

	// init BroadcastReqListener
	broadcastReqListener := BroadcastReqListener{
		watchdog: w,
	}
	go broadcastReqListener.Start(&broadcastReqListener, nil)

	// respListenerChan
	respListenerChan := make(chan string)

	// init BroadcastRespListener
	broadcastRespListener := BroadcastRespListener{
		watchdog:         w,
		respListenerChan: respListenerChan,
	}
	go broadcastRespListener.Start(&broadcastRespListener, nil)

	// init BroadcastServer
	broadcastServer := BroadcastServer{
		watchdog:         w,
		respListenerChan: respListenerChan,
	}
	go broadcastServer.Start(&broadcastServer, nil)

	// init MetaCacheSyncer
	metaCacheSyncer := MetaCacheSyncer{
		watchdog: w,
	}
	go metaCacheSyncer.Start(&metaCacheSyncer, nil)

	// init Sweeper
	sweeper := Sweeper{
		watchdog: w,
	}
	go sweeper.Start(&sweeper, nil)

	// init Rebalancer
	rebalancer := Rebalancer{
		watchdog:  w,
		semaphore: NewSemaphore(MAX_CONCURRENT_REBALANCE),
	}
	go rebalancer.Start(&rebalancer, nil)

	// init RebalanceServer
	rebalanceServer := RebalanceServer{
		watchdog: w,
	}
	go rebalanceServer.Start(&rebalanceServer, nil)

	// init RebalanceGC
	rebalanceGC := RebalanceGC{
		watchdog:  w,
		semaphore: NewSemaphore(MAX_CONCURRENT_GC),
	}
	go rebalanceGC.Start(&rebalanceGC, nil)

	// init AdminServer
	adminServer := AdminServer{
		watchdog: w,
	}
	go adminServer.Start(&adminServer, nil)

	// wait
	for {
		time.Sleep(1e8)
	}

	logger.Critical("Swarm stopped.")
}
