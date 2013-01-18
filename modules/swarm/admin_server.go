package swarm

import (
	"encoding/json"
	"net/http"
	"time"
)

//------------------------------------------------------------------------------
// AdminServer
//------------------------------------------------------------------------------

// TODO: auth admin request

type AdminResponse struct {
	Version   string
	Timestamp time.Time
	Status    int
	Message   string
}

type AdminServer struct {
	WorkerBase
	watchdog *Watchdog
}

// Implement interface Restartable
func (this *AdminServer) Name() string {
	return "AdminServer"
}

// Implement interface Restartable
func (this *AdminServer) Start(r Restartable, e interface{}) {
	defer func() {
		e := recover()
		this.OnPanic(this, this.watchdog, e)
	}()

	// do stuff
	logger.Critical("AdminServer started.")

	http.Handle(URL_SWARM_ADMIN_REBALANCE, httpHandler(handleAdminRebalance))
	http.Handle(URL_SWARM_ADMIN_OFFLINE_DEVICE, httpHandler(handleAdminOfflineDevice))
	http.Handle(URL_SWARM_ADMIN_ONLINE_DEVICE, httpHandler(handleAdminOnlineDevice))
	http.Handle(URL_SWARM_ADMIN_REMOVE_DEVICE, httpHandler(handleAdminRemoveDevice))
	http.Handle(URL_SWARM_ADMIN_DEL_ALARM, httpHandler(handleAdminDelAlarm))
	http.Handle(URL_SWARM_ADMIN_JOIN_NODE, httpHandler(handleAdminJoinNode))
	http.Handle(URL_SWARM_ADMIN_REMOVE_NODE, httpHandler(handleAdminRemoveNode))

	err := http.ListenAndServe(NODE_ADDRESS+":"+ADMIN_PORT, nil)
	checkFatal(err)

	logger.Critical("AdminServer stopped.")
}

// Handle admin rebalance
func handleAdminRebalance(writer http.ResponseWriter, req *http.Request) *SwarmError {
	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"peer", verifyNothing},
		ParamVerifier{"device", verifyDeviceId},
	})
	if swarmError != nil {
		return swarmError
	}
	peer, deviceId := params[0], params[1]

	// build response
	response := AdminResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	go adminRebalance(peer, deviceId)

	// finish
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}
	return nil
}

// Handle admin offline device
// This only effect runtime status
func handleAdminOfflineDevice(writer http.ResponseWriter, req *http.Request) *SwarmError {
	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"device", verifyNothing},
	})
	if swarmError != nil {
		return swarmError
	}
	deviceId := params[0]

	// build response
	response := AdminResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	deviceState, ok := LOCAL_DEVICES.GetState(deviceId)
	if !ok {
		return SwarmErrorf(http.StatusInternalServerError, "Invalid DOMAIN_MAP to get local device %s", deviceId)
	}

	if deviceState.Status != DEVICE_STATUS_ONLINE {
		return SwarmErrorf(http.StatusBadRequest, "Local device %s is not online", deviceId)
	}

	logger.Info("Admin offline device %s", deviceId)

	LOCAL_DEVICES.SetStatus(deviceId, DEVICE_STATUS_OFFLINE)
	NODE_SETTINGS.Save()

	// finish
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}
	return nil
}

// Handle admin online device
func handleAdminOnlineDevice(writer http.ResponseWriter, req *http.Request) *SwarmError {
	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"device", verifyNothing},
	})
	if swarmError != nil {
		return swarmError
	}
	deviceId := params[0]

	// build response
	response := AdminResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	deviceState, ok := LOCAL_DEVICES.GetState(deviceId)
	if !ok {
		return SwarmErrorf(http.StatusInternalServerError, "Invalid DOMAIN_MAP to get local device %s", deviceId)
	}
	if deviceState.Status == DEVICE_STATUS_ONLINE {
		return SwarmErrorf(http.StatusBadRequest, "Local device %s is online already", deviceId)
	}

	logger.Info("Admin online device %s", deviceId)
	// weight and path is updated in updateLocalDevices

	// publish since domain map changed
	LOCAL_DEVICES.SetStatus(deviceId, DEVICE_STATUS_ONLINE)
	if deviceState.Status == DEVICE_STATUS_REMOVED {
		LOCAL_DEVICES.OnChanged()
	} else {
		// just save when from offline
		NODE_SETTINGS.Save()
	}

	// finish
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}
	return nil
}

// Handle admin remove device
func handleAdminRemoveDevice(writer http.ResponseWriter, req *http.Request) *SwarmError {
	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"device", verifyNothing},
	})
	if swarmError != nil {
		return swarmError
	}
	deviceId := params[0]

	// build response
	response := AdminResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	deviceState, ok := LOCAL_DEVICES.GetState(deviceId)
	if !ok {
		return SwarmErrorf(http.StatusInternalServerError, "Invalid DOMAIN_MAP to get local device %s", deviceId)
	}
	if deviceState.Status == DEVICE_STATUS_REMOVED {
		return SwarmErrorf(http.StatusBadRequest, "Local device %s is removed already", deviceId)
	}

	logger.Info("Admin removed device %s", deviceId)

	// notify changes
	LOCAL_DEVICES.SetStatus(deviceId, DEVICE_STATUS_REMOVED)
	LOCAL_DEVICES.OnChanged()

	// finish
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}
	return nil
}

// Handle admin del alarm
func handleAdminDelAlarm(writer http.ResponseWriter, req *http.Request) *SwarmError {
	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"id", verifyNothing},
	})
	if swarmError != nil {
		return swarmError
	}
	id := params[0]

	// build response
	response := AdminResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	err := ALARM_MANAGER.Del(id)
	if err != nil {
		response.Status = STATUS_ERROR
		response.Message = err.Error()
	}

	// finish
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}
	return nil
}

// Handle admin join node, must call on join node.
func handleAdminJoinNode(writer http.ResponseWriter, req *http.Request) *SwarmError {
	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// build response
	response := AdminResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	if err := DOMAIN_MAP.AdminJoin(); err != nil {
		response.Status = STATUS_ERROR
		response.Message = err.Error()
	}

	logger.Info("Admin joined local node")

	// finish
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}
	return nil
}

// Handle admin remove node, must not call on removed node.
func handleAdminRemoveNode(writer http.ResponseWriter, req *http.Request) *SwarmError {
	// check request
	if req.Method != "GET" {
		return SwarmErrorf(http.StatusMethodNotAllowed, "Not allowed method: %s", req.Method)
	}

	// process params
	swarmError, params := processParams(req, []ParamVerifier{
		ParamVerifier{"node", verifyNothing},
	})
	if swarmError != nil {
		return swarmError
	}
	nodeIpRemote := params[0]

	if nodeIpRemote == NODE_ADDRESS {
		return SwarmErrorf(http.StatusBadRequest, "Not to remove node itself.")
	}

	// build response
	response := AdminResponse{
		Version:   SWARM_VERSION,
		Timestamp: time.Now(),
		Status:    STATUS_OK,
	}

	if err := DOMAIN_MAP.AdminRemove(nodeIpRemote); err != nil {
		response.Status = STATUS_ERROR
		response.Message = err.Error()
	}
	logger.Info("Admin removed node %s", nodeIpRemote)

	// finish
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(response); err != nil {
		return SwarmErrorf(http.StatusInternalServerError, "Encode error: %s", err.Error())
	}
	return nil
}
