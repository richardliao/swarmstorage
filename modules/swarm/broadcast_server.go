package swarm

import (
	"bytes"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"time"
)

type Task struct {
	TaskId   string
	Expire   time.Time
	ReqChan  chan string
	RespChan chan string
	AckChan  chan string
}

type BroadcastRequest struct {
	TaskId         string
	BlockAnyDevice BlockInfo
}

type BroadcastResponse struct {
	Version  string
	Status   int
	TaskId   string
	NodeIp   string
	DeviceId string
}

// Global
var REGISTER_CHAN chan Task

func init() {
	REGISTER_CHAN = make(chan Task)
}

//------------------------------------------------------------------------------
// BroadcastReqListener
//------------------------------------------------------------------------------

/*
Broadcast is used for nodes to find block for read/del not in hashring.
Broadcast use UDP multicast.
Several parts participate in broadcast handling:
    requester: the reqeust origin
    broadcastServer: handle broadcast tasks and transfer message between chans
    broadcastReqListener: handle broadcast request in remote nodes
    broadcastRespListener: handle broadcast response from remote nodes

A task is consisted of:
    ReqChan: requester to broadcastServer
    RespChan: broadcastServer to requester
    AckChan: requester to broadcastServer

The process is:

The requester first build a broadcast task and register it with a RespChan.

broadcastServer receive the task and queue it. Get task from queue and 
broadcast it out.

broadcastReqListener receive broadcast request, check and response to 
broadcastRespListener if block exists.

broadcastRespListener receive response and add it to respListenerChan, which 
will be received by broadcastServer, then send it back to RespChan of task. 

Requester get result and ack broadcastServer in AckChan of task, and do further
for the result.

After get ack, broadcastServer will clean the task.
*/
type BroadcastReqListener struct {
	WorkerBase
	watchdog *Watchdog
}

// Implement interface Restartable
func (this *BroadcastReqListener) Name() string {
	return "BroadcastReqListener"
}

// Implement interface Restartable
func (this *BroadcastReqListener) Start(r Restartable, e interface{}) {
	defer func() {
		e := recover()
		this.OnPanic(this, this.watchdog, e)
	}()

	logger.Critical("BroadcastReqListener started.")

	// listen
	raddr := &net.UDPAddr{net.ParseIP(MULTICAST_ADDR), BROADCAST_REQ_PORT}
	//socket, err := net.ListenMulticastUDP("udp", ifi, raddr)
	socket, err := net.ListenMulticastUDP("udp", nil, raddr)
	checkFatal(err)

	// handle request
	for {
		// get block query request
		buff := make([]byte, 4096)
		num, remoteAddr, err := socket.ReadFromUDP(buff)
		if err != nil {
			logger.Error("Failed ReadFromUDP, error: %s", err.Error())
			continue
		}

		// parse request
		var request BroadcastRequest
		err = json.Unmarshal(buff[:num], &request)
		if err != nil {
			logger.Error("Failed decode broadcast request from %s, error: %s", remoteAddr.IP, err.Error())
			continue
		}
		logger.Debug("Get broadcast from %s req: %q", remoteAddr.IP, request)
		MONITOR.SetVariable("M:BR:ParseRequest", 1)

		// handle block query request
		deviceId, err := findBlockDevice(request.BlockAnyDevice)
		if err != nil {
			// do not response
			continue
		}
		//logger.Debug("find block on device: %s", deviceId)

		// build response
		response := BroadcastResponse{
			Version:  SWARM_VERSION,
			Status:   STATUS_OK,
			TaskId:   request.TaskId,
			NodeIp:   NODE_ADDRESS,
			DeviceId: deviceId,
		}

		// send response
		logger.Debug("Send broadcast %q, resp: %q", remoteAddr.IP, response)

		laddr := &net.UDPAddr{
			IP:   net.ParseIP(NODE_ADDRESS),
			Port: 0,
		}
		raddr := &net.UDPAddr{
			IP:   remoteAddr.IP,
			Port: BROADCAST_RESP_PORT,
		}
		socket, err := net.DialUDP("udp4", laddr, raddr)
		if err != nil {
			logger.Error("Failed send broadcast, error: %s", err.Error())
			continue
		}

		encoder := json.NewEncoder(socket)
		if err := encoder.Encode(response); err != nil {
			logger.Error("Broadcast response error: %s", err.Error())
			socket.Close()
			continue
		}
		socket.Close()
		MONITOR.SetVariable("M:BR:SendResponse", 1)
	}
}

//------------------------------------------------------------------------------
// BroadcastRespListener
//------------------------------------------------------------------------------

type BroadcastRespListener struct {
	WorkerBase
	watchdog         *Watchdog
	respListenerChan chan string
}

// Implement interface Restartable
func (this *BroadcastRespListener) Name() string {
	return "BroadcastRespListener"
}

// Implement interface Restartable
func (this *BroadcastRespListener) Start(r Restartable, e interface{}) {
	defer func() {
		e := recover()
		this.OnPanic(this, this.watchdog, e)
	}()

	logger.Critical("BroadcastRespListener started.")

	socket, err := net.ListenUDP("udp4", &net.UDPAddr{
		IP:   net.ParseIP(NODE_ADDRESS),
		Port: BROADCAST_RESP_PORT,
	})
	checkFatal(err)

	for {
		buff := make([]byte, 4096)
		num, remoteAddr, err := socket.ReadFromUDP(buff)
		if err != nil {
			logger.Error("Falied read broadcast from %s, error: ", remoteAddr, err.Error())
			continue
		}
		this.respListenerChan <- string(buff[:num])
	}
}

//------------------------------------------------------------------------------
// BroadcastServer
//------------------------------------------------------------------------------

type BroadcastServer struct {
	WorkerBase
	watchdog         *Watchdog
	respListenerChan chan string
}

// Implement interface Restartable
func (this *BroadcastServer) Name() string {
	return "BroadcastServer"
}

// Implement interface Restartable
func (this *BroadcastServer) Start(r Restartable, e interface{}) {
	defer func() {
		e := recover()
		this.OnPanic(this, this.watchdog, e)
	}()

	logger.Critical("BroadcastServer started.")

	//// start broadcastRespListener
	//// TODO: nonblocking
	//respListenerChan := make(chan string)
	//go broadcastRespListener(respListenerChan)

	// select
	tasks := make(map[string]Task)

	for {
		select {
		case resp := <-this.respListenerChan:
			MONITOR.SetVariable("M:BR:GetResponse", 1)
			// parse response
			var response BroadcastResponse
			decoder := json.NewDecoder(bytes.NewBufferString(resp))
			if err := decoder.Decode(&response); err != nil {
				logger.Error("Failed decode broadcast response, error: %s", err.Error())
				continue
			}

			// handle response
			task, ok := tasks[response.TaskId]
			if !ok {
				logger.Error("Failed get task %s", response.TaskId)
				continue
			}

			// write result
			task.RespChan <- resp

		case task := <-REGISTER_CHAN:
			//logger.Debug("broadcastServer get task")
			// register task 
			tasks[task.TaskId] = task

		default:
			if len(tasks) == 0 {
				// no task
				//logger.Debug("broadcastServer wait")
				time.Sleep(1e8)
				continue
			}

			// do tasks
			busy := false
			for taskId, task := range tasks {
				select {
				// get req
				case req := <-task.ReqChan:
					busy = true
					//logger.Debug("broadcastServer task %q req: %q", taskId, req)

					// broadcast
					err := this.broadcastReq(taskId, req)
					if err != nil {
						// TODO: notify requester
						task.RespChan <- this.buildErrorResponse(taskId)
					}
					continue
				// wait ack
				case <-task.AckChan:
					busy = true
					//logger.Debug("broadcastServer get ack")
					// del task
					delete(tasks, taskId)
					continue
				default:
					if time.Now().After(task.Expire) {
						// handle expired task
						//logger.Debug("Expired task: %s", taskId)
						// write expire result
						task.RespChan <- this.buildErrorResponse(taskId)
						continue
					} else {
						// next task
						continue
					}
				}
			}

			if !busy {
				//logger.Debug("broadcastServer not busy")
				time.Sleep(1e8)
			}
		}
	}
}

// Build error response
func (this *BroadcastServer) buildErrorResponse(taskId string) string {
	response := BroadcastResponse{
		Version:  SWARM_VERSION,
		Status:   STATUS_ERROR,
		TaskId:   taskId,
		NodeIp:   "",
		DeviceId: "",
	}
	resp, _ := json.Marshal(response)
	return string(resp)
}

// Broadcast request
func (this *BroadcastServer) broadcastReq(taskId string, req string) (err error) {
	MONITOR.SetVariable("M:BR:BroadcastReq", 1)

	// get broadcast addr
	//broadcastAddr := getBroadcastAddr()

	laddr := &net.UDPAddr{
		IP:   net.ParseIP(NODE_ADDRESS),
		Port: 0,
	}
	raddr := &net.UDPAddr{
		IP:   net.ParseIP(MULTICAST_ADDR),
		Port: BROADCAST_REQ_PORT,
	}

	socket, err := net.DialUDP("udp4", laddr, raddr)
	defer socket.Close()
	if err != nil {
		logger.Error("Broadcast dailup error: %s", err.Error())
		return err
	}

	_, err = socket.Write([]byte(req))
	if err != nil {
		logger.Error("Broadcast write error: %s", err.Error())
		return err
	}

	return nil
}

// Get broadcast address
func (this *BroadcastServer) getBroadcastAddr() (broadcastAddr net.IP) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		logger.Error("Failed get InterfaceAddrs, error: %s", err.Error())
		return
	}

	for _, addr := range addrs {
		ip, network, err := net.ParseCIDR(addr.String())
		if err != nil {
			logger.Error("Failed ParseCIDR %s, error: %s", addr.String(), err.Error())
			continue
		}

		if ip.String() == NODE_ADDRESS {
			parts := strings.SplitN(network.String(), "/", 2)
			ones, err := strconv.ParseInt(parts[1], 10, 8)
			if err != nil {
				logger.Error("Failed parse NODE_ADDRESS %s, error: %s", ip.String(), err.Error())
				continue
			}
			mask := net.CIDRMask(int(ones), 32)

			broadcastAddr = net.IPv4(net.IPv4bcast[12]^mask[0]|ip[12],
				net.IPv4bcast[13]^mask[1]|ip[13],
				net.IPv4bcast[14]^mask[2]|ip[14],
				net.IPv4bcast[15]^mask[3]|ip[15])
			break
		}
	}
	return
}
