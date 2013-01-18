package swarm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"syscall"
	"time"
)

//------------------------------------------------------------------------------
// MonitorServer
//------------------------------------------------------------------------------

type MonitorServer struct {
	WorkerBase
	watchdog *Watchdog
}

// Implement interface Restartable
func (this *MonitorServer) Name() string {
	return "MonitorServer"
}

// Implement interface Restartable
func (this *MonitorServer) Start(r Restartable, e interface{}) {
	defer func() {
		e := recover()
		this.OnPanic(this, this.watchdog, e)
	}()

	// do stuff
	logger.Critical("MonitorServer started.")

	http.Handle(URL_SWARM_MONITOR_VERSION, httpHandler(handleMonitorVersion))
	http.Handle(URL_SWARM_MONITOR_QUERY, httpHandler(handleMonitorQuery))
	http.Handle(URL_SWARM_MONITOR_ALL, httpHandler(handleMonitorAll))
	http.Handle(URL_SWARM_MONITOR_SYSINFO, httpHandler(handleMonitorSysinfo))
	http.Handle(URL_SWARM_MONITOR_VARIABLE, httpHandler(handleMonitorVariable))
	http.Handle(URL_SWARM_MONITOR_ALARM, httpHandler(handleMonitorAlarm))

	err := http.ListenAndServe(NODE_ADDRESS+":"+MONITOR_PORT, nil)
	checkFatal(err)

	logger.Critical("MonitorServer stopped.")
}

// Handle monitor version
// Return version in plaintext
func handleMonitorVersion(writer http.ResponseWriter, req *http.Request) *SwarmError {
	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	writer.Write([]byte(SWARM_VERSION))
	return nil
}

// Handle monitor query
type MonitorQueryResponse struct {
	Version   string
	Timestamp time.Time
	Variable  *SSVariable
}

func handleMonitorQuery(writer http.ResponseWriter, req *http.Request) *SwarmError {
	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"name", verifyNothing},
	})
	if swarmError != nil {
		return swarmError
	}
	name := params[0]

	ssv, err := MONITOR.ReadVariable(name)
	if err != nil {
		logger.Debug("Failed read variable %s, error: %s", name, err.Error())
		return SwarmErrorf(http.StatusBadRequest, err.Error())
	}

	// build response
	response := MonitorQueryResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Variable:  ssv,
	}

	// finish
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}
	return nil
}

// Handle monitor query all
type MonitorAllResponse struct {
	Version   string
	Timestamp time.Time
	Variables map[string]*SSVariable
}

func handleMonitorAll(writer http.ResponseWriter, req *http.Request) *SwarmError {
	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// build response
	response := MonitorAllResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Variables: MONITOR.ssData,
	}

	// finish
	result, err := json.MarshalIndent(response, "", " ")
	if err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}

	writer.Write(result)
	return nil
}

/*
{
 "Version": "0.1",
 "Timestamp": 1343361078576367000,
 "Ncpu": 2,
 "Sysinfo": {
  "Uptime": 839399,
  "Loads": [
   49568,
   47104,
   44800
  ],
  "Totalram": 2109562880,
  "Freeram": 203055104,
  "Sharedram": 0,
  "Bufferram": 287592448,
  "Totalswap": 1999626240,
  "Freeswap": 1789079552,
  "Procs": 492,
  "Pad": 0,
  "Totalhigh": 1214808064,
  "Freehigh": 40607744,
  "Unit": 1,
  "X_f": [
   0,
   0,
   0,
   0,
   0,
   0,
   0,
   0
  ]
 },
 "Rusage": {
  "Utime": {
   "Sec": 0,
   "Usec": 100006
  },
  "Stime": {
   "Sec": 0,
   "Usec": 56003
  },
  "Maxrss": 5408,
  "Ixrss": 0,
  "Idrss": 0,
  "Isrss": 0,
  "Minflt": 1454,
  "Majflt": 0,
  "Nswap": 0,
  "Inblock": 0,
  "Oublock": 16,
  "Msgsnd": 0,
  "Msgrcv": 0,
  "Nsignals": 0,
  "Nvcsw": 2395,
  "Nivcsw": 58
 },
 "Devices": {
  "589d0ce36f40407bb79fadf84c7411f1": {
   "Type": 61267,
   "Bsize": 4096,
   "Blocks": 7596115,
   "Bfree": 3450071,
   "Bavail": 3064211,
   "Files": 1929536,
   "Ffree": 1905532,
   "Fsid": {
    "X__val": [
     423621017,
     1394364693
    ]
   },
   "Namelen": 255,
   "Frsize": 4096,
   "Flags": 0,
   "Spare": [
    0,
    0,
    0,
    0
   ]
  }
 }
}
*/
// Handle monitor query sysinfo
type MonitorSysinfoResponse struct {
	Version   string
	Timestamp time.Time
	Ncpu      int
	Sysinfo   *syscall.Sysinfo_t
	Rusage    *syscall.Rusage
	Devices   map[string]*syscall.Statfs_t
}

func handleMonitorSysinfo(writer http.ResponseWriter, req *http.Request) *SwarmError {
	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	sysinfo, _ := GetSysinfo()
	rusage, _ := GetRusage()

	devices := make(map[string]*syscall.Statfs_t)
	for deviceId := range LOCAL_DEVICES.Data {
		statfs, _ := GetDiskStatfs(deviceId)
		devices[deviceId] = statfs
	}

	// build response
	response := MonitorSysinfoResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Ncpu:      NCPU,
		Sysinfo:   sysinfo,
		Rusage:    rusage,
		Devices:   devices,
	}

	// finish
	result, err := json.MarshalIndent(response, "", " ")
	if err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}

	writer.Write(result)
	return nil
}

// Handle monitor query global variable
type MonitorVariableResponse struct {
	Version           string
	Timestamp         time.Time
	DomainMap         *DomainMap
	DeviceMapping     map[string]string
	LocalDevices      *LocalDevices
	NodeReliabilities map[string]*NodeReliable
	RepairStatus      RepairStatus
	MetaCache         MetaCache
	RebalanceCache    string
	DomainConfig       map[string]interface{}
	ScoreSegs         []string
	PrimaryScoreSegs  []string
}

func handleMonitorVariable(writer http.ResponseWriter, req *http.Request) *SwarmError {
	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"name", verifyNothing},
	})
	if swarmError != nil {
		return swarmError
	}
	name := params[0]

	// build response
	response := MonitorVariableResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
	}

	switch name {
	case "DomainMap":
		response.DomainMap = DOMAIN_MAP
	case "DeviceMapping":
		response.DeviceMapping = DEVICE_MAPPING
	case "LocalDevices":
		response.LocalDevices = LOCAL_DEVICES
	case "NodeReliabilities":
		response.NodeReliabilities = NODE_RELIABILITIES
	case "RepairStatus":
		response.RepairStatus = REPAIR_STATUS
	case "MetaCache":
		response.MetaCache = META_CACHE
	case "RebalanceCache":
		response.RebalanceCache = fmt.Sprintf("%s", REBALANCE_CACHE)
	case "DomainConfig":
		response.DomainConfig = getDomainConfig()
	case "ScoreSegs":
		deviceIdLocal, err := randomDevice()
		if err != nil {
			return SwarmErrorf(http.StatusInternalServerError, "%s", err.Error())
		}
		// get seg
		scoreSegs, err := REBALANCE_CACHE.GetRepairScoreSegs(deviceIdLocal)
		if err != nil {
			return SwarmErrorf(http.StatusInternalServerError, "%s", err.Error())
		}
		segs := make([]string, 0)
		for scoreSeg := range scoreSegs {
			segs = append(segs, fmt.Sprintf("%#v", scoreSeg))
		}
		sort.Strings(segs)
		response.ScoreSegs = segs
	case "PrimaryScoreSegs":
		deviceIdLocal, err := randomDevice()
		if err != nil {
			return SwarmErrorf(http.StatusInternalServerError, "%s", err.Error())
		}
		// get device weight
		deviceState, ok := LOCAL_DEVICES.GetState(deviceIdLocal)
		if !ok {
			return SwarmErrorf(http.StatusInternalServerError, "")
		}

		// get seg
		scores := GetNodeScores(deviceIdLocal, DOMAIN_RING_SPOTS, deviceState.Weight)
		segs := make([]string, 0)
		for _, score := range scores {
			segs = append(segs, fmt.Sprintf("%08x", score))
		}
		sort.Strings(segs)
		response.PrimaryScoreSegs = segs
	}

	// finish
	result, err := json.MarshalIndent(response, "", " ")
	if err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}

	writer.Write(result)
	return nil
}

// Handle monitor alarm
type MonitorAlarmResponse struct {
	Version   string
	Timestamp time.Time
	Alarms    map[string]Alarm
}

func handleMonitorAlarm(writer http.ResponseWriter, req *http.Request) *SwarmError {
	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// build response
	response := MonitorAlarmResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Alarms:    ALARM_MANAGER.Alarms,
	}

	// finish
	result, err := json.MarshalIndent(response, "", " ")
	if err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}

	writer.Write(result)
	return nil
}
