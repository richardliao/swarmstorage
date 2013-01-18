package fs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/richardliao/swarm/modules/swarm"
	"github.com/richardliao/swarm/third_party/github.com/hanwen/go-fuse/fuse"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
)

// Read object from swarm
func readObject(objecthash string, swarmServers string, offset int64) (body []byte, err error) {
	v := url.Values{}
	v.Set("objecthash", objecthash)
	v.Set("blocktype", swarm.BLOCK_TYPE_INDEX)
	v.Set("offset", swarm.Itoa(offset))

	u := url.URL{
		Scheme:   "http",
		Host:     swarmServers + ":" + swarm.OSS_PORT,
		Path:     swarm.URL_SWARM_READ_OSS,
		RawQuery: v.Encode(),
	}

	// prepare request
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		logger.Error("Read %s error %s", objecthash, err.Error())
		return
	}

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		logger.Error("Read %s error %s", objecthash, err.Error())
		return
	}
	if resp.Status != "200 OK" {
		logger.Error("Read %s error status %s", objecthash, resp.Status)
		return
	}

	// var buf [512]byte
	reader := resp.Body
	defer reader.Close()
	body, err = ioutil.ReadAll(reader)
	if err != nil {
		logger.Error("Read %s error %s", objecthash, err.Error())
		return
	}

	return body, nil
}

// Query object status from swarm
func queryObject(objecthash string, swarmServers string) (response swarm.ObjectStatusResponse, err error) {
	v := url.Values{}
	v.Set("objecthash", objecthash)
	v.Set("blocktype", swarm.BLOCK_TYPE_INDEX)

	u := url.URL{
		Scheme:   "http",
		Host:     swarmServers + ":" + swarm.OSS_PORT,
		Path:     swarm.URL_SWARM_QUERY_OSS,
		RawQuery: v.Encode(),
	}

	// prepare request
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		logger.Info("Query %s error %s", objecthash, err.Error())
		return
	}

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		logger.Info("Query %s error %s", objecthash, err.Error())
		return
	}
	if resp.Status != "200 OK" {
		logger.Info("Query %s error status %s", objecthash, resp.Status)
		err = fmt.Errorf("Invalid object")
		return
	}

	// var buf [512]byte
	reader := resp.Body
	defer reader.Close()
	body, err := ioutil.ReadAll(reader)

	// parse response
	err = json.Unmarshal(body, &response)
	if err != nil {
		return
	}
	if response.Status != swarm.STATUS_OK {
		logger.Info("Query %s error status %s", objecthash, response.Status)
		err = fmt.Errorf("Invalid object")
		return
	}

	return response, nil
}

// Write object to swarm
func writeObject(objecthash string, data []byte, swarmServers string) (err error) {
	v := url.Values{}
	v.Set("objecthash", objecthash)
	v.Set("blocktype", swarm.BLOCK_TYPE_INDEX)

	u := url.URL{
		Scheme:   "http",
		Host:     swarmServers + ":" + swarm.OSS_PORT,
		Path:     swarm.URL_SWARM_WRITE_OSS,
		RawQuery: v.Encode(),
	}

	// prepare request
	body := bytes.NewBuffer(data)
	req, err := http.NewRequest("POST", u.String(), body)
	if err != nil {
		logger.Error("Write %s error %s", objecthash, err.Error())
		return
	}

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		logger.Error("Write %s error %s", objecthash, err.Error())
		return
	}
	if resp.Status != "200 OK" {
		logger.Error("Write %s error status %s", objecthash, resp.Status)
		err = fmt.Errorf("Failed write object")
		return
	}

	var buf [512]byte
	reader := resp.Body
	defer reader.Close()
	for {
		n, err := reader.Read(buf[0:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		fmt.Print(string(buf[0:n]))
	}
	return nil
}

// Set attr
func setAttr(name string, swarmServers string) (a *fuse.Attr, err error) {
	// query object
	response, err := queryObject(name, swarmServers)
	if err != nil {
		// fillNewChildAttr need a valid fi and ignore err when create
		return
	}
	//logger.Debug("queryObject response %#v", response)

	// convert ctime
	ctime := uint64(response.CTime.UnixNano() / 1e9)

	// new attr
	a = newAttr(response.ObjectSize, ctime)

	return a, nil
}

// Gen attr
func newAttr(size int64, ctime uint64) (a *fuse.Attr) {
	a = &fuse.Attr{}
	a.Ino = uint64(1193659)
	a.Size = uint64(size)
	a.Blocks = uint64(1)
	a.Atime = ctime
	a.Atimensec = uint32(0)
	a.Mtime = ctime
	a.Mtimensec = uint32(0)
	a.Ctime = ctime
	a.Ctimensec = uint32(0)
	a.Mode = uint32(33188 | 0666)
	a.Nlink = uint32(1)
	a.Uid = uint32(os.Getuid())
	a.Gid = uint32(os.Getgid())
	a.Rdev = uint32(0)
	a.Blksize = uint32(4096)
	return
}
