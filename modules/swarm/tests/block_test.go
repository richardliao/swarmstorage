package tests

import (
	"bytes"
	"fmt"
	"github.com/richardliao/swarm/modules/swarm"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
)

func TestWriteBlock(t *testing.T) {

	v := url.Values{}
	v.Set("deviceid", DeviceId)
	v.Set("blockhash", TestBlockhash)
	v.Set("blocktype", TestIndexhash)

	u := url.URL{
		Scheme:   "http",
		Host:     TestServer + ":" + swarm.BLOCK_PORT,
		Path:     swarm.URL_SWARM_WRITE_BLOCK,
		RawQuery: v.Encode(),
	}

	// prepare request
	body := bytes.NewBuffer(TestBlock)
	req, err := http.NewRequest("POST", u.String(), body)
	req.Header.Add("Content-Length", string(len(TestBlock)))
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		return
	}

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(resp)
	if resp.Status != "200 OK" {
		fmt.Println(resp.Status)
		return
	}

	var buf [512]byte
	reader := resp.Body
	defer reader.Close()
	fmt.Println("got body")
	for {
		n, err := reader.Read(buf[0:])
		if err != nil {
			return
		}
		fmt.Print(string(buf[0:n]))
	}
}

func TestReadBlock(t *testing.T) {
	v := url.Values{}
	v.Set("deviceid", DeviceId)
	v.Set("blockhash", TestBlockhash)
	v.Set("blocktype", TestIndexhash)

	u := url.URL{
		Scheme:   "http",
		Host:     TestServer + ":" + swarm.BLOCK_PORT,
		Path:     swarm.URL_SWARM_READ_BLOCK,
		RawQuery: v.Encode(),
	}

	// prepare requesT
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		return
	}

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(resp)
	if resp.Status != "200 OK" {
		fmt.Println(resp.Status)
		return
	}

	// var buf [512]byte
	reader := resp.Body
	defer reader.Close()
	fmt.Println("got body")
	body, err := ioutil.ReadAll(reader)
	fmt.Println(string(body))

	if !bytes.Equal(TestBlock, body) {
		t.Errorf("Mismatch read block: %v", body)
	}
}
