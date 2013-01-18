package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/richardliao/swarm/modules/swarm"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// global
var TestServer = "192.168.200.1"
var DeviceId = "589d0ce36f40407bb79fadf84c7411f1"
var TestFile = []byte("0123456789abcdefghijklm")
var TestBlock = TestFile[:4]

var TestIndexhash string
var TestObjecthash string
var TestBlockhash string

func init() {
	// init NODE_BASE_PATH
	swarm.NODE_BASE_PATH = "/data/swarm/node1"

	// load global vars
	swarm.PreStart()

	TestIndexhash = GetIndexhash(TestFile)
	TestObjecthash = swarm.Sha1Hex([]byte(TestFile))
	TestBlockhash = swarm.Sha1Hex([]byte(TestBlock))
}

// For tests
// Get object hash from test file
func GetIndexhash(objectContent []byte) (objecthash string) {
	blockhashes := make([]string, 0)
	size := len(objectContent)
	var start int
	for start = 0; start <= size; start += swarm.BLOCK_SIZE {
		end := start + swarm.BLOCK_SIZE
		if end > size {
			end = size
		}
		block := objectContent[start:end]
		blockhash := swarm.Sha1Hex(block)
		blockhashes = append(blockhashes, blockhash)
		if end == size {
			break
		}
	}
	return swarm.Sha1Hex([]byte(strings.Join(blockhashes, "")))
}

// Do http request
func RequestHttp(nodeIp string, port string, path string, query url.Values,
	method string, body *[]byte, verifyJsonResponse bool, timeout time.Duration, contentLength int64) (pdata *[]byte, err error) {

	u := url.URL{
		Scheme:   "http",
		Host:     nodeIp + ":" + port,
		Path:     path,
		RawQuery: query.Encode(),
	}

	// prepare request
	buff := bytes.NewBuffer([]byte(""))
	if body != nil {
		buff = bytes.NewBuffer(*body)
	}
	req, err := http.NewRequest(method, u.String(), buff)
	if method == "POST" && contentLength != 0 {
		req.Header.Add("Content-Length", swarm.Itoa(contentLength))
	}
	if err != nil {
		return
	}

	respChan := make(chan swarm.HttpResponse)

	go func() {
		result := swarm.HttpResponse{}

		client := &http.Client{}

		resp, err := client.Do(req)
		if err != nil {
			result.Error = err
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
			return
		}
		pdata = result.Response
	case <-time.After(timeout):
		err = fmt.Errorf("Timeout: http://%s:%s%s", nodeIp, port, path)
		return
	}

	if verifyJsonResponse {
		var response swarm.BaseResponse
		// parse response
		if err = json.Unmarshal(*pdata, &response); err != nil {
			return
		}

		// check status
		if response.Status != swarm.STATUS_OK {
			msg := fmt.Sprintf("node %s, error: %s", nodeIp, err.Error())
			err = fmt.Errorf(msg)
			return
		}
		return pdata, nil
	}
	return pdata, nil
}
