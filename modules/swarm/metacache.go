package swarm

import (
	"fmt"
	"strings"
	"sync"
)

// Global

var REBALANCE_CACHE RebalanceCache

func init() {
	REBALANCE_CACHE = NewRebalanceCache()
}

//------------------------------------------------------------------------------
// TypeBlocks
//------------------------------------------------------------------------------

type TypeBlocks struct {
	lock *sync.RWMutex
	Data map[string]*BlockList // {blocktype: BlockList}
}

func NewTypeBlocks() *TypeBlocks {
	return &TypeBlocks{
		lock: new(sync.RWMutex),
		Data: make(map[string]*BlockList),
	}
}

func (this *TypeBlocks) Get(blocktype string) (blockList *BlockList, ok bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	blockList, ok = this.Data[blocktype]
	return
}

func (this *TypeBlocks) Set(blocktype string, blockList *BlockList) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.Data[blocktype] = blockList
}

func (this *TypeBlocks) Delete(blocktype string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.Data, blocktype)
}

//------------------------------------------------------------------------------
// MetaCache
//------------------------------------------------------------------------------

type MetaCache struct {
	lock  *sync.RWMutex
	Ready bool
	Data  map[string]*TypeBlocks // {deviceid: {blocktype: BlockList}}
}

func NewMetaCache() MetaCache {
	return MetaCache{
		lock:  new(sync.RWMutex),
		Ready: false,
		Data:  make(map[string]*TypeBlocks),
	}
}

func (this MetaCache) Get(deviceId string) (typeBlocks *TypeBlocks, ok bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	typeBlocks, ok = this.Data[deviceId]
	return
}

func (this MetaCache) Set(deviceId string, typeBlocks *TypeBlocks) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.Data[deviceId] = typeBlocks
}

func (this MetaCache) getOrNewTypeBlocks(blockInfo BlockInfo) (typeBlocks *TypeBlocks) {
	typeBlocks, ok := this.Get(blockInfo.Device)
	if !ok {
		typeBlocks = NewTypeBlocks()
		for _, blocktype := range AVAILABLE_BLOCK_TYPES {
			typeBlocks.Set(blocktype, NewBlockList())
		}
		this.Set(blockInfo.Device, typeBlocks)
	}
	return typeBlocks
}

// Get blockList
func (this MetaCache) getOrNewBlockList(blockInfo BlockInfo) (blockList *BlockList) {
	typeBlocks := this.getOrNewTypeBlocks(blockInfo)

	blockList, ok := typeBlocks.Get(blockInfo.Type)
	if !ok {
		blockList = NewBlockList()
		typeBlocks.Set(blockInfo.Type, blockList)
	}
	return blockList
}

func (this MetaCache) DelBlockhash(blockInfo BlockInfo) {
	if _, ok := this.Get(blockInfo.Device); !ok {
		logger.Debug("Invalid MetaCache get device %s", blockInfo.Device)
		return
	}

	blockList := this.getOrNewBlockList(blockInfo)
	if !blockList.Contains(blockInfo.Hash) {
		logger.Debug("Invalid MetaCache get block %s", blockInfo)
		return
	}
	blockList.Delete(blockInfo.Hash)

	REBALANCE_CACHE.Expire(blockInfo)
}

// Check if block exist
func (this MetaCache) BlockInMetacache(blockInfo BlockInfo) bool {
	if _, ok := this.Get(blockInfo.Device); !ok {
		return false
	}
	blockList := this.getOrNewBlockList(blockInfo)
	return blockList.Contains(blockInfo.Hash)
}

// Find the device a block located on
func (this MetaCache) FindBlockDevice(blockAnyDevice BlockInfo) (deviceId string) {
	for deviceId, typeBlocks := range this.Data {
		for _, blockList := range typeBlocks.Data {
			if blockList.Contains(blockAnyDevice.Hash) {
				blockInfoTarget := NewBlockInfo(deviceId, blockAnyDevice.Type, blockAnyDevice.Hash)
				blockPath, err := getBlockPath(blockInfoTarget)
				if err != nil {
					continue
				}
				if ExistPath(blockPath) {
					return deviceId
				}
			}
		}
	}
	return ""
}

// Save META_CACHE
func (this MetaCache) Save(blockInfo BlockInfo) {
	if !blockInfo.Verify() {
		logger.Debug("Invalid block %s", blockInfo)
		return
	}

	blockList := this.getOrNewBlockList(blockInfo)
	if !blockList.Contains(blockInfo.Hash) {
		REBALANCE_CACHE.Expire(blockInfo)
	}

	blockList.Set(blockInfo.Hash)

	MONITOR.SetVariable("M:MC:SaveMetaCache", 1)
}

// Parse block filename key
func parseBlockFileName(blockFileName string) (blockAnyDevice BlockInfo, err error) {
	parts := strings.SplitN(blockFileName, ".", 2)
	if len(parts) != 2 {
		err = fmt.Errorf("Invalid meta key: %s", blockFileName)
		return
	}
	blockhash, blocktype := parts[0], parts[1]
	if blocktype != BLOCK_TYPE_BLOCK && blocktype != BLOCK_TYPE_INDEX && blocktype != BLOCK_TYPE_OBJECT {
		err = fmt.Errorf("Invalid meta key: %s", blockFileName)
		return
	}
	blockAnyDevice = NewBlockAnyDevice(blocktype, blockhash)

	return blockAnyDevice, nil
}
