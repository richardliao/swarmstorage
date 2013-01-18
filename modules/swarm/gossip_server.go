package swarm

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// Global
var ACCEPTABLE_VERSIONS map[string]string

func init() {
	ACCEPTABLE_VERSIONS = make(map[string]string)
	ACCEPTABLE_VERSIONS[SWARM_VERSION] = ""
}

// Check if remote node version acceptable
func isAcceptalbeVersion(version string) bool {
	if _, ok := ACCEPTABLE_VERSIONS[version]; ok {
		return true
	}
	return false
}

//------------------------------------------------------------------------------
// GossipServer
//------------------------------------------------------------------------------

/*
Serve gossip request, return local node states.
Serve node verify request, return timestamp and local devices.
We use gob for serialization.
*/
type GossipServer struct {
	WorkerBase
	watchdog *Watchdog
}

// Implement interface Restartable
func (this *GossipServer) Name() string {
	return "GossipServer"
}

// Implement interface Restartable
func (this *GossipServer) Start(r Restartable, e interface{}) {
	defer func() {
		e := recover()
		this.OnPanic(this, this.watchdog, e)
	}()

	logger.Critical("GossipServer started.")

	http.Handle(URL_SWARM_GOSSIP_PROBE, httpHandler(handleGossipProbe))
	http.Handle(URL_SWARM_GOSSIP_VERIFY, httpHandler(handleGossipVerify))
	http.Handle(URL_SWARM_GOSSIP_NOTIFY, httpHandler(handleGossipNotify))

	err := http.ListenAndServe(NODE_ADDRESS+":"+GOSSIP_PORT, nil)
	checkFatal(err)

	logger.Critical("GossipServer stopped.")
}

// Handle gossip probe request
type GossipProbeResponse struct {
	Version         string
	Timestamp       time.Time
	Status          int
	NodeCert        *NodeCert
	NodeSig         []byte
	DomainConfigSig string
	DomainMap       *DomainMap
}

func handleGossipProbe(writer http.ResponseWriter, req *http.Request) *SwarmError {
	MONITOR.SetVariable("M:GOS:Probe", 1)

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"version", verifyNothing},
	})
	if swarmError != nil {
		return swarmError
	}
	version := params[0]

	if !isAcceptalbeVersion(version) {
		ALARM_MANAGER.Add(ALARM_INCOMPATIBLE_VERSION, fmt.Errorf("Incompatible version %s node %s", version, IPFromAddr(req.RemoteAddr)), "Incompatible version")
		return SwarmErrorf(http.StatusBadRequest, "Incompatible version: %s", version)
	}

	response := GossipProbeResponse{
		Version:         SWARM_VERSION,
		Timestamp:       time.Now(),
		Status:          STATUS_OK,
		NodeCert:        NODE_CERT,
		NodeSig:         NODE_SIG,
		DomainConfigSig: DOMAIN_CONFIG_SIG,
		DomainMap:       DOMAIN_MAP,
	}

	encoder := gob.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		logger.Error("Failed encode gossip response, error ", err.Error())
	}
	return nil
}

// Probe node
func requestGossipProbe(nodeIp string) {
	// throttle
	throttle("requestGossipProbe", "")

	query := url.Values{
		"version": []string{SWARM_VERSION},
	}

	pdata, err := RequestHttp(nodeIp, GOSSIP_PORT, URL_SWARM_GOSSIP_PROBE, query, "GET", nil, false, GOSSIP_TIMEOUT, false)
	if err != nil {
		DOMAIN_MAP.RecordDownNode(nodeIp)
		return
	}

	response := new(GossipProbeResponse)
	if err := gob.NewDecoder(bytes.NewReader(*pdata)).Decode(response); err != nil {
		logger.Error("Failed decode gossip response from node %s, error: %s", nodeIp, err.Error())
		DOMAIN_MAP.RecordDownNode(nodeIp)
		return
	}

	// verify cert
	if err := verifyGossipProbeResponse(response, nodeIp); err != nil {
		logger.Error("Failed verify gossip response from node %s, error: %s", nodeIp, err.Error())
		DOMAIN_MAP.RecordDownNode(nodeIp)
		return
	}

	// verify timestamp
	if time.Now().Sub(response.Timestamp) > MAX_NODE_TIME_DIFF {
		ALARM_MANAGER.Add(ALARM_CLOCK_DRIFT, fmt.Errorf(""), "Clock exceed limit from node %s", nodeIp)
		// make node untrust
		DOMAIN_MAP.RecordDownNode(nodeIp)
		return
	}

	// merge
	DOMAIN_MAP.MergeDomainMap(nodeIp, response.DomainMap)
}

// Handle verify info request
type GossipVerifyResponse struct {
	Version         string
	Timestamp       time.Time
	Status          int
	VerifyNodeState NodeState
}

func handleGossipVerify(writer http.ResponseWriter, req *http.Request) *SwarmError {
	MONITOR.SetVariable("M:GOS:Verify", 1)

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"version", verifyNothing},
	})
	if swarmError != nil {
		return swarmError
	}
	version := params[0]

	if !isAcceptalbeVersion(version) {
		return SwarmErrorf(http.StatusBadRequest, "Invalid version: %s", version)
	}

	response := GossipVerifyResponse{
		Version:         SWARM_VERSION,
		Timestamp:       time.Now(),
		Status:          STATUS_OK,
		VerifyNodeState: *DOMAIN_MAP.LocalNodeState(),
	}

	encoder := gob.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		logger.Error("Failed encode verify response, error ", err.Error())
	}
	return nil
}

// Verify node status
func requestGossipVerify(nodeIp string) (verifiedNodeState *NodeState, err error) {
	defer onRecoverablePanic()

	query := url.Values{
		"version": []string{SWARM_VERSION},
	}

	pdata, err := RequestHttp(nodeIp, GOSSIP_PORT, URL_SWARM_GOSSIP_VERIFY, query, "GET", nil, false, GOSSIP_TIMEOUT, false)
	if err != nil {
		logger.Error("Failed request gossip verify node %s, error: %s", nodeIp, err.Error())
		return
	}

	response := new(GossipVerifyResponse)
	if err = gob.NewDecoder(bytes.NewReader(*pdata)).Decode(response); err != nil {
		logger.Error("Failed decode gossip verify response from node %s, error: %s", nodeIp, err.Error())
		return
	}

	if response.Status != STATUS_OK {
		err = fmt.Errorf("Request gossip verify node %s with bad status", nodeIp)
		return
	}

	if time.Now().Sub(response.Timestamp) > MAX_NODE_TIME_DIFF {
		ALARM_MANAGER.Add(ALARM_CLOCK_DRIFT, fmt.Errorf(""), "Clock exceed limit from node %s", nodeIp)
		err = fmt.Errorf("Clock exceed limit from node %s", nodeIp)
		return
	}

	verifiedNodeState = &response.VerifyNodeState

	return
}

// Handle gissip notify request
func handleGossipNotify(writer http.ResponseWriter, req *http.Request) *SwarmError {
	MONITOR.SetVariable("M:GOS:Notify", 1)

	response := BaseResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"version", verifyNothing},
		ParamVerifier{"source", verifyNothing},
	})
	if swarmError != nil {
		return swarmError
	}
	version, nodeIp := params[0], params[1]

	if !isAcceptalbeVersion(version) {
		return SwarmErrorf(http.StatusBadRequest, "Invalid version: %s", version)
	}

	// send pull gossip
	GOSSIP_NOTIFIER.Add(nodeIp)

	encoder := gob.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		logger.Error("Failed encode notify response, error ", err.Error())
	}

	return nil
}

// Request gossip notify
func requestGossipNotify(nodeIp string) (err error) {
	defer onRecoverablePanic()

	if DOMAIN_MAP.IsLocalNodeRemoved() {
		err = fmt.Errorf("No gossip notify when local node is removed.")
		return
	}

	query := url.Values{
		"version": []string{SWARM_VERSION},
		"source":  []string{NODE_ADDRESS},
	}

	pdata, err := RequestHttp(nodeIp, GOSSIP_PORT, URL_SWARM_GOSSIP_NOTIFY, query, "GET", nil, false, GOSSIP_TIMEOUT, false)
	if err != nil {
		logger.Error("Failed request gossip notify node %s, error: %s", nodeIp, err.Error())
		return
	}

	response := new(BaseResponse)
	if err = gob.NewDecoder(bytes.NewReader(*pdata)).Decode(response); err != nil {
		logger.Error("Failed decode gossip notify response from node %s, error: %s", nodeIp, err.Error())
		return
	}

	if response.Status != STATUS_OK {
		err = fmt.Errorf("Failed request gossip notify node %s", nodeIp)
		logger.Error("Failed request gossip notify node %s, error: %s", nodeIp, err.Error())
		return
	}

	MONITOR.SetVariable("M:GOS:Notify", 1)

	return nil
}
