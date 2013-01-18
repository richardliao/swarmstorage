package swarm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// http://localhost:8082/debug/pprof
import _ "net/http/pprof"

//------------------------------------------------------------------------------
// Http handler
//------------------------------------------------------------------------------

type httpHandler func(http.ResponseWriter, *http.Request) *SwarmError

func (fn httpHandler) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	// setup recover
	defer func() {
		if e := recover(); e != nil {
			logger.Fatal("Server error: %s", e)
			http.Error(writer, "Server error", http.StatusInternalServerError)
		}
	}()

	if e := fn(writer, req); e != nil { // e is *SwarmError
		logger.Error("SwarmError: %s", e.Message)
		http.Error(writer, e.Message, e.Code)
	}
}

//------------------------------------------------------------------------------
// BaseResponse
//------------------------------------------------------------------------------

type BaseResponse struct {
	Version   string
	Timestamp time.Time
	Status    int
	Desc      string
}

//------------------------------------------------------------------------------
// Request http
//------------------------------------------------------------------------------

type HttpResponse struct {
	Response *[]byte
	Error    error
	Down     bool
}

// Do http request
func RequestHttp(nodeIp string, port string, path string, query url.Values,
	method string, body *[]byte, verifyJsonResponse bool, timeout time.Duration,
	triggerProbe bool) (pdata *[]byte, err error) {

	u := url.URL{
		Scheme:   "http",
		Host:     nodeIp + ":" + port,
		Path:     path,
		RawQuery: query.Encode(),
	}

	// prepare request
	buff := bytes.NewBuffer([]byte(""))
	contentLength := 0
	if body != nil {
		buff = bytes.NewBuffer(*body)
		contentLength = len(*body)
	}
	req, err := http.NewRequest(method, u.String(), buff)
	if method == "POST" {
		req.Header.Add("Content-Length", Itoa(contentLength))
	}
	if err != nil {
		return
	}

	respChan := make(chan HttpResponse)

	go func() {
		result := HttpResponse{}

		client := &http.Client{}

		resp, err := client.Do(req)
		if err != nil {
			result.Error = err
			// remote maybe down
			result.Down = true
			respChan <- result
			return
		}
		defer resp.Body.Close()

		if resp.Status != "200 OK" {
			msg := fmt.Sprintf("node %s resp status: %s", nodeIp, resp.Status)
			result.Error = fmt.Errorf(msg)
			respChan <- result
			return
		}

		_temp, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			result.Error = err
			respChan <- result
			return
		}

		result.Response = &_temp
		respChan <- result
	}()

	// blocking wait
	select {
	case result := <-respChan:
		if result.Error != nil {
			err = result.Error
			if triggerProbe && result.Down {
				// trigger probe
				GOSSIP_NOTIFIER.Add(nodeIp)
			}
			return
		}
		pdata = result.Response
		MONITOR.SetVariable(fmt.Sprintf("M:NET:%s%s", nodeIp, path), 1)
	case <-time.After(timeout):
		MONITOR.SetVariable("M:NET:Timeout", 1)
		err = fmt.Errorf("Timeout: http://%s:%s%s", nodeIp, port, path)
		// trigger probe
		if triggerProbe {
			GOSSIP_NOTIFIER.Add(nodeIp)
		}
		return
	}

	if verifyJsonResponse {
		var response BaseResponse
		// parse response
		if err = json.Unmarshal(*pdata, &response); err != nil {
			MONITOR.SetVariable(fmt.Sprintf("M:ERR:%s%s", nodeIp, path), 1)
			logger.Error("Decode error: %s", err.Error())
			return
		}

		// check status
		if response.Status != STATUS_OK {
			msg := fmt.Sprintf("node %s, error: %s", nodeIp, err.Error())
			logger.Error(msg)
			err = fmt.Errorf(msg)
			return
		}
		return pdata, nil
	}
	return pdata, nil
}

//------------------------------------------------------------------------------
// Verify params
//------------------------------------------------------------------------------

type ParamVerifier struct {
	ParamName string
	Verifier  func(string, *http.Request) error
}

// Process params
func processParams(req *http.Request, paramVerifiers []ParamVerifier) (swarmError *SwarmError, params []string) {
	req.ParseForm()

	// get params
	paramNames := make([]string, len(paramVerifiers))
	for i, paramVerifier := range paramVerifiers {
		paramNames[i] = paramVerifier.ParamName
	}
	params, err := parseParams(req, paramNames)
	if err != nil {
		swarmError = SwarmErrorf(http.StatusBadRequest, err.Error())
		return
	}

	// verify
	for i, paramVerifier := range paramVerifiers {
		if err = paramVerifier.Verifier(params[i], req); err != nil {
			swarmError = SwarmErrorf(http.StatusBadRequest, err.Error())
			return
		}
	}
	return nil, params
}

// Verify nothing
func verifyNothing(p string, req *http.Request) (err error) {
	return nil
}

// Verify objecthash
func verifyObjecthash(objecthash string, req *http.Request) (err error) {
	if len(objecthash) != HASH_HEX_SIZE || strings.ToLower(objecthash) != objecthash {
		return fmt.Errorf("Invalid objecthash %s", objecthash)
	}
	return nil
}

// Verify object blocktype
func verifyObjectBlocktype(blocktype string, req *http.Request) (err error) {
	if blocktype != BLOCK_TYPE_INDEX && blocktype != BLOCK_TYPE_OBJECT {
		return fmt.Errorf("Invalid blocktype %s", blocktype)
	}
	return nil
}

// Verify blockhash
func verifyBlockhash(blockhash string, req *http.Request) (err error) {
	if len(blockhash) != HASH_HEX_SIZE || strings.ToLower(blockhash) != blockhash {
		return fmt.Errorf("Invalid blockhash %s", blockhash)
	}
	return nil
}

// Verify blocktype
func verifyBlocktype(blocktype string, req *http.Request) (err error) {
	if blocktype != BLOCK_TYPE_BLOCK && blocktype != BLOCK_TYPE_INDEX && blocktype != BLOCK_TYPE_OBJECT {
		return fmt.Errorf("Invalid blocktype %s", blocktype)
	}
	return nil
}

// Verify replica
func verifyReplica(replicaStr string, req *http.Request) (err error) {
	if _, err = strconv.Atoi(replicaStr); err != nil {
		return
	}
	return nil
}

// Verify device id
func verifyDeviceId(deviceId string, req *http.Request) (err error) {
	if deviceId == DEVICE_ID_ALL {
		return nil
	}
	if !LOCAL_DEVICES.IsLocalDevice(deviceId) {
		return fmt.Errorf("Not exist device: %s", deviceId)
	}
	return nil
}

// Verify version
func verifyVersion(version string, req *http.Request) (err error) {
	if !isAcceptalbeVersion(version) {
		ALARM_MANAGER.Add(ALARM_INCOMPATIBLE_VERSION, fmt.Errorf("Incompatible version %s node %s", version, IPFromAddr(req.RemoteAddr)), "Incompatible version")
		return fmt.Errorf("Invalid version: %s", version)
	}
	return nil
}

// Verify priority
func verifyPriority(priorityStr string, req *http.Request) (err error) {
	if _, err = strconv.Atoi(priorityStr); err != nil {
		return
	}
	return nil
}

// Verify score
// TODO: check overflow
func verifyScore(scoreStr string, req *http.Request) (err error) {
	if _, err = strconv.Atoi(scoreStr); err != nil {
		return
	}
	return nil
}

// Verify local node
func verifyLocalNode(nodeIp string, req *http.Request) (err error) {
	if nodeIp != NODE_ADDRESS {
		return fmt.Errorf("Not local node %s", nodeIp)
	}
	return nil
}
