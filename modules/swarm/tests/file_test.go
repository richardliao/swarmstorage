package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/richardliao/swarm/modules/swarm"
	"net/url"
	"testing"
)

// Object

func TestWriteObject(t *testing.T) {
	query := url.Values{
		"objecthash": []string{TestObjecthash},
		"blocktype":  []string{swarm.BLOCK_TYPE_OBJECT},
	}

	body := TestFile
	pdata, err := RequestHttp(TestServer, swarm.OSS_PORT, swarm.URL_SWARM_WRITE_OSS, query, "POST", &body, false, swarm.WRITE_BLOCK_TIMEOUT, int64(len(body)))
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println(string(*pdata))

	// parse response
	var response swarm.ObjectWriteResponse
	if err = json.Unmarshal(*pdata, &response); err != nil {
		t.Errorf("Decode error: %s", err.Error())
		return
	}

	// check status
	if response.Status != swarm.STATUS_OK || response.Objecthash != TestObjecthash {
		t.Errorf("Failed TestWriteObject")
		return
	}
}

func TestReadObject(t *testing.T) {
	offset := 1
	query := url.Values{
		"objecthash": []string{TestObjecthash},
		"blocktype":  []string{swarm.BLOCK_TYPE_OBJECT},
		"offset":     []string{swarm.Itoa(offset)},
	}

	pdata, err := RequestHttp(TestServer, swarm.OSS_PORT, swarm.URL_SWARM_READ_OSS, query, "GET", nil, false, swarm.READ_BLOCK_TIMEOUT, 0)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println(string(*pdata))

	if !bytes.Equal([]byte(TestFile[offset:]), *pdata) {
		t.Errorf("Mismatch TestReadObject: %v", *pdata)
	}
}

func TestQueryObject(t *testing.T) {
	query := url.Values{
		"objecthash": []string{TestObjecthash},
		"blocktype":  []string{swarm.BLOCK_TYPE_OBJECT},
	}

	pdata, err := RequestHttp(TestServer, swarm.OSS_PORT, swarm.URL_SWARM_QUERY_OSS, query, "GET", nil, false, swarm.READ_BLOCK_TIMEOUT, 0)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println(string(*pdata))

	// parse response
	var response swarm.ObjectStatusResponse
	if err = json.Unmarshal(*pdata, &response); err != nil {
		t.Errorf("Decode error: %s", err.Error())
		return
	}

	// check status
	if response.Status != swarm.STATUS_OK {
		t.Errorf("Failed TestQueryObject")
		return
	}
}

// Index
func TestWriteIndex(t *testing.T) {

	query := url.Values{
		"objecthash": []string{TestIndexhash},
		"blocktype":  []string{swarm.BLOCK_TYPE_INDEX},
	}

	body := TestFile
	pdata, err := RequestHttp(TestServer, swarm.OSS_PORT, swarm.URL_SWARM_WRITE_OSS, query, "POST", &body, false, swarm.WRITE_BLOCK_TIMEOUT, int64(len(body)))
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println(string(*pdata))

	// parse response
	var response swarm.ObjectWriteResponse
	if err = json.Unmarshal(*pdata, &response); err != nil {
		t.Errorf("Decode error: %s", err.Error())
		return
	}

	// check status
	if response.Status != swarm.STATUS_OK || response.Objecthash != TestIndexhash {
		t.Errorf("Failed TestWriteIndex")
		return
	}
}

func TestWriteStreaming(t *testing.T) {
	query := url.Values{
		"objecthash": []string{""},
		"blocktype":  []string{swarm.BLOCK_TYPE_INDEX},
	}

	body := TestFile
	pdata, err := RequestHttp(TestServer, swarm.OSS_PORT, swarm.URL_SWARM_WRITE_OSS, query, "POST", &body, false, swarm.WRITE_BLOCK_TIMEOUT, 0)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println(string(*pdata))

	// parse response
	var response swarm.ObjectWriteResponse
	if err = json.Unmarshal(*pdata, &response); err != nil {
		t.Errorf("Decode error: %s", err.Error())
		return
	}

	// check status
	if response.Status != swarm.STATUS_OK || response.Objecthash != TestIndexhash {
		t.Errorf("Failed TestWriteStreaming")
		return
	}
}

func TestModifyIndex(t *testing.T) {
	offset := 1
	query := url.Values{
		"objecthash": []string{""},
		"blocktype":  []string{swarm.BLOCK_TYPE_INDEX},
		"baseobject": []string{TestIndexhash},
		"offset":     []string{swarm.Itoa(offset)},
	}

	body := TestFile[offset:]
	pdata, err := RequestHttp(TestServer, swarm.OSS_PORT, swarm.URL_SWARM_WRITE_OSS, query, "POST", &body, false, swarm.WRITE_BLOCK_TIMEOUT, int64(len(body)))
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println(string(*pdata))

	// parse response
	var response swarm.ObjectWriteResponse
	if err = json.Unmarshal(*pdata, &response); err != nil {
		t.Errorf("Decode error: %s", err.Error())
		return
	}

	// check status
	if response.Status != swarm.STATUS_OK || response.Objecthash != TestIndexhash {
		t.Errorf("Failed TestModifyIndex")
		return
	}
}

func TestReadIndex(t *testing.T) {
	offset := 1
	query := url.Values{
		"objecthash": []string{TestIndexhash},
		"blocktype":  []string{swarm.BLOCK_TYPE_INDEX},
		"offset":     []string{swarm.Itoa(offset)},
	}

	pdata, err := RequestHttp(TestServer, swarm.OSS_PORT, swarm.URL_SWARM_READ_OSS, query, "GET", nil, false, swarm.READ_BLOCK_TIMEOUT, 0)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println(string(*pdata))

	if !bytes.Equal([]byte(TestFile[offset:]), *pdata) {
		t.Errorf("Mismatch TestReadIndex: %v", *pdata)
	}
}

func TestQueryIndex(t *testing.T) {
	query := url.Values{
		"objecthash": []string{TestIndexhash},
		"blocktype":  []string{swarm.BLOCK_TYPE_INDEX},
	}

	pdata, err := RequestHttp(TestServer, swarm.OSS_PORT, swarm.URL_SWARM_QUERY_OSS, query, "GET", nil, false, swarm.READ_BLOCK_TIMEOUT, 0)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	fmt.Println(string(*pdata))

	// parse response
	var response swarm.ObjectStatusResponse
	if err = json.Unmarshal(*pdata, &response); err != nil {
		t.Errorf("Decode error: %s", err.Error())
		return
	}

	// check status
	if response.Status != swarm.STATUS_OK {
		t.Errorf("Failed TestQueryIndex")
		return
	}
}
