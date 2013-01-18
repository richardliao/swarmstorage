package tests

import (
	"fmt"
	"github.com/richardliao/swarm/modules/swarm"
	//	"strconv"
	"testing"
)

func TestHashring(t *testing.T) {
	ring := swarm.NewRing(200)

	nodes := map[string]int{
		"test1.server.com": 1,
		"test2.server.com": 1,
		"test3.server.com": 2,
		"test4.server.com": 5,
	}

	for k, v := range nodes {
		ring.AddNode(k, v)
	}

	ring.Finish()

	m := make(map[string]int)
	for i := 0; i < 100; i++ {
		//tick, _ := ring.GetPrimaryTick("test value" + strconv.FormatUint(uint64(i), 10))
		//m[tick.name]++
	}

	for k, _ := range nodes {
		fmt.Println(k, m[k])
	}
}
