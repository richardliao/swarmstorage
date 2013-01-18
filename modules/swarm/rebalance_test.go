package swarm

import (
	//	"fmt"
	//	"strconv"
	"testing"

//	"time"
//	"sync"
)

/*
blocks:
177746f96133b928638c220aa77e786663891958.i    63891958
25bf58983b8ab103fa88b4032503fc8b65651ca1.b    65651ca1
72b14a1712c677f1b2f8cbf025fce1437c5735cb.b    7c5735cb
83787f060a59493aefdcd4b2369990e7303e186e.b    303e186e
c4b5c86bd577da3d93fea7c89cba61c78b48e589.b    8b48e589
c64fae3e196f818c8b2a488da4197a5d9cd6f094.b    9cd6f094
e13ac15a30aaa8c841b369b1e0ca4d4d79ffd8cf.b    79ffd8cf

ring all:
2 "{From:0x078896bb, To:0x194b07e6}",
4a"{From:0x194b07e6, To:0x20ec9b63}",
3 "{From:0x20ec9b63, To:0x2a7d788e}",
1 "{From:0x2a7d788e, To:0x2c334b68}",
2 "{From:0x2c334b68, To:0x3ada966e}",
3 "{From:0x3ada966e, To:0x6511e9ad}",
4b"{From:0x6511e9ad, To:0x7f25ba31}",
4a"{From:0x7f25ba31, To:0x83a6bc80}",
1 "{From:0x83a6bc80, To:0x8c94620a}",
3 "{From:0x8c94620a, To:0x8def7b67}",
1 "{From:0x8def7b67, To:0x912b36a6}",
2 "{From:0x912b36a6, To:0x9856d192}",
4b"{From:0x9856d192, To:0x9ae02497}",
1 "{From:0x9ae02497, To:0xa0774fc8}",
4a"{From:0xa0774fc8, To:0xa4b6cff4}",
4a"{From:0xa4b6cff4, To:0xa79870ed}",
4b"{From:0xa79870ed, To:0xb6c52657}",
4b"{From:0xb6c52657, To:0xf8c2b654}",
2 "{From:0xf8c2b654, To:0xfab3f05a}",
3 "{From:0xfab3f05a, To:0x078896bb}",

ring 4a:
4a"{From:0x194a07e6, To:0x20ec9b63}",
3 "{From:0x20ec9b63, To:0x2a7d788e}",
1 "{From:0x2a7d788e, To:0x2c334b68}",
4a"{From:0x7f25ba31, To:0x83a6bc80}",
1 "{From:0x83a6bc80, To:0x8c94620a}",
3 "{From:0x8c94620a, To:0x8def7b67}",
1 "{From:0x8def7b67, To:0x912b36a6}",
4a"{From:0xa0774fc8, To:0xa4c6cff4}",
4a"{From:0xa4c6cff4, To:0xa79870ed}",

ring 4c gc:
2 "{From:0x078896bb, To:0x194a07e6}",
2 "{From:0x2c334b68, To:0x3ada966e}",
3 "{From:0x3ada966e, To:0x6511e9ad}",
4b"{From:0x6511e9ad, To:0x7f25ba31}",
2 "{From:0x912b36a6, To:0x9856d192}",
4b"{From:0x9856d192, To:0x9ae02497}",
1 "{From:0x9ae02497, To:0xa0774fc8}",
4b"{From:0xa79870ed, To:0xb6c52657}",
4b"{From:0xb6c52657, To:0xf8c2b654}",
2 "{From:0xf8c2b654, To:0xfab3f05a}",
3 "{From:0xfab3f05a, To:0x078896bb}",
*/

func TestRebalanceRing(t *testing.T) {
	initGlobal("192.168.200.4")

	// test data
	// node4a
	deviceIdLocal := "043c9e161a884caa9b1c1fd51757322e"
	blocktype := BLOCK_TYPE_BLOCK

	blockhash := "c64fae3e196f818c8b2a488da4197a5d9cd6f094" // score: 9cd6f094

	// save to META_CACHE
	blockInfo := NewBlockInfo(deviceIdLocal, blocktype, blockhash)
	META_CACHE.Save(blockInfo)

	// scoreSegs for test
	scoreSegsTest := NewScoreSegSet()
	scoreSegsTest.Add(ScoreSeg{0x194a07e6, 0x20ec9b63})
	scoreSegsTest.Add(ScoreSeg{0x20ec9b63, 0x2a7d788e})
	scoreSegsTest.Add(ScoreSeg{0x2a7d788e, 0x2c334b68})
	scoreSegsTest.Add(ScoreSeg{0x7f25ba31, 0x83a6bc80})
	scoreSegsTest.Add(ScoreSeg{0x83a6bc80, 0x8c94620a})
	scoreSegsTest.Add(ScoreSeg{0x8c94620a, 0x8def7b67})
	scoreSegsTest.Add(ScoreSeg{0x8def7b67, 0x912b36a6})
	scoreSegsTest.Add(ScoreSeg{0xa0774fc8, 0xa4c6cff4})
	scoreSegsTest.Add(ScoreSeg{0xa4c6cff4, 0xa79870ed})

	scoreSegs, err := REBALANCE_CACHE.GetRepairScoreSegs(deviceIdLocal)
	if err != nil {
		t.Errorf(err.Error())
	}

	for scoreSeg := range scoreSegs {
		if !scoreSegsTest.Contains(scoreSeg) {
			t.Errorf("scoreSeg %#v\n", scoreSeg)
		}
	}

	for scoreSeg := range scoreSegsTest.Data {
		if _, ok := scoreSegs[scoreSeg]; !ok {
			t.Errorf("scoreSeg %#v\n", scoreSeg)
		}
	}
}

func TestRebalanceSync(t *testing.T) {
	initGlobal("192.168.200.1")

	// test data
	// node1
	deviceIdLocal := "6e6e5d2fb5034f9f916efa4f1eeb37e6"
	blocktype := BLOCK_TYPE_BLOCK

	blockhash := "c4a5c86bd577da3d93fea7c89cba61c78b48e589" // score: 8b48e589

	// save to META_CACHE
	blockInfo := NewBlockInfo(deviceIdLocal, blocktype, blockhash)
	META_CACHE.Save(blockInfo)

	// scoreSegs for test
	scoreSegsTest := NewScoreSegSet()
	scoreSegsTest.Add(ScoreSeg{0x2a7d788e, 0x2c334b68})
	scoreSegsTest.Add(ScoreSeg{0x2c334b68, 0x3ada966e})
	scoreSegsTest.Add(ScoreSeg{0x3ada966e, 0x6511e9ad})
	scoreSegsTest.Add(ScoreSeg{0x83a6bc80, 0x8c94620a})
	scoreSegsTest.Add(ScoreSeg{0x8c94620a, 0x8def7b67})
	scoreSegsTest.Add(ScoreSeg{0x8def7b67, 0x912b36a6})
	scoreSegsTest.Add(ScoreSeg{0x912b36a6, 0x9856d192})
	scoreSegsTest.Add(ScoreSeg{0x9856d192, 0x9ae02497})
	scoreSegsTest.Add(ScoreSeg{0x9ae02497, 0xa0774fc8})
	scoreSegsTest.Add(ScoreSeg{0xa0774fc8, 0xa4c6cff4})
	scoreSegsTest.Add(ScoreSeg{0xa4c6cff4, 0xa79870ed})
	scoreSegsTest.Add(ScoreSeg{0xa79870ed, 0xb6c52657})
	scoreSegsTest.Add(ScoreSeg{0xb6c52657, 0xf8c2b654})
	scoreSegsTest.Add(ScoreSeg{0xf8c2b654, 0xfab3f05a})

	scoreSegs, err := REBALANCE_CACHE.GetRepairScoreSegs(deviceIdLocal)
	if err != nil {
		t.Errorf(err.Error())
	}

	for scoreSeg := range scoreSegs {
		if !scoreSegsTest.Contains(scoreSeg) {
			t.Errorf("scoreSeg %#v\n", scoreSeg)
		}
	}

	for scoreSeg := range scoreSegsTest.Data {
		if _, ok := scoreSegs[scoreSeg]; !ok {
			t.Errorf("scoreSeg %#v\n", scoreSeg)
		}
	}

	// get primaryScore
	primaryScore := uint32(0x83a6bc80)
	primaryScores := GetNodeScores(deviceIdLocal, DOMAIN_RING_SPOTS, 2)
	if primaryScores[0] != primaryScore {
		t.Errorf("Not match primaryScores %#v", primaryScores)
	}

	// get scoreSeg
	scoreSeg, err := NewScoreSeg(primaryScore)
	if err != nil {
		t.Errorf(err.Error())
	}

	// SyncDigest
	remoteSyncDigest := SyncDigest{}
	localSyncDigest, err := REBALANCE_CACHE.GetSyncDigest(deviceIdLocal, scoreSeg, blocktype)
	if err != nil {
		t.Errorf(err.Error())
	}

	syncBlocks, err := getSyncBlocks(scoreSeg, deviceIdLocal, blocktype, localSyncDigest, remoteSyncDigest)
	if err != nil {
		t.Errorf(err.Error())
	}
	if syncBlocks.Len() == 0 {
		t.Errorf("Empty syncBlocks")
	}
}

func initGlobal(nodeIp string) {
	NODE_BASE_PATH = "/data/swarm/node1"
	LoadDomainConfig()

	// init MONITOR
	w := NewWatchdog()
	MONITOR = Monitor{
		watchdog:     w,
		ssData:       make(map[string]*SSVariable),
		analysisData: make(map[string]*AnalysisData),
		valueChan:    make(chan *SSValue, 1000),
		commandChan:  make(chan *MonitorCommand),
	}

	NODE_ADDRESS = nodeIp
	DOMAIN_MAP = initDomainMap()

	// DOMAIN_RING & DEVICE_MAPPING
	DOMAIN_RING = NewRing(DOMAIN_RING_SPOTS)
	for nodeIp, nodeState := range DOMAIN_MAP.Data {
		// add devices
		for deviceId, weight := range nodeState.Devices {
			DOMAIN_RING.AddNode(deviceId, *weight)
			DEVICE_MAPPING[deviceId] = nodeIp
		}
	}
	DOMAIN_RING.Finish()

	// REBALANCE_CACHE
	REBALANCE_CACHE.Update()
}

func initDomainMap() *DomainMap {
	// init
	domainMap := NewDomainMap()

	// add
	var nodeState *NodeState

	nodeState = NewNodeState()
	nodeState.SetDevice("6e6e5d2fb5034f9f916efa4f1eeb37e6", 2)
	domainMap.Set("192.168.200.1", nodeState)

	nodeState = NewNodeState()
	nodeState.SetDevice("cb0f6257e34141cfa6dee9d807034d2a", 2)
	domainMap.Set("192.168.200.2", nodeState)

	nodeState = NewNodeState()
	nodeState.SetDevice("115d8ded8687412596bad1fc34c29342", 2)
	domainMap.Set("192.168.200.3", nodeState)

	nodeState = NewNodeState()
	nodeState.SetDevice("043c9e161a884caa9b1c1fd51757322e", 2)
	nodeState.SetDevice("72cbac6eb3cf4bfe83813f61c6ce0af0", 2)
	domainMap.Set("192.168.200.4", nodeState)

	return domainMap
}
