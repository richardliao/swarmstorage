package swarm

import (
	"github.com/richardliao/swarm/third_party/github.com/ryszard/goskiplist/skiplist"
	"sync"
)

//------------------------------------------------------------------------------
// BlockList
//------------------------------------------------------------------------------

type BlockList struct {
	lock *sync.Mutex
	Data *skiplist.SkipList // score: BlockBucket
}

func NewBlockList() *BlockList {
	return &BlockList{
		lock: new(sync.Mutex),
		Data: skiplist.NewCustomMap(func(l, r interface{}) bool {
			return l.(uint32) < r.(uint32)
		}),
	}
}

// Get block bucket
func (this *BlockList) getBlockBucket(blockhash string) (blockBucket *StringSet, ok bool) {
	score, err := GetBlockScore(blockhash)
	if err != nil {
		return blockBucket, false
	}

	got, ok := this.Data.Get(score)
	if !ok {
		return
	}

	return got.(*StringSet), true
}

// Check contains blockhash
func (this *BlockList) Contains(blockhash string) (ok bool) {
	blockBucket, ok := this.getBlockBucket(blockhash)
	if !ok {
		return false
	}

	if blockBucket.Contains(blockhash) {
		return true
	}
	return false
}

// Add blockhash
func (this *BlockList) Set(blockhash string) {
	this.lock.Lock()
	defer this.lock.Unlock()

	blockBucket, ok := this.getBlockBucket(blockhash)
	if !ok {
		score, err := GetBlockScore(blockhash)
		if err != nil {
			return
		}
		blockBucket = NewStringSet()
		this.Data.Set(score, blockBucket)
	}

	blockBucket.Add(blockhash)
}

// Delete blockhash
func (this *BlockList) Delete(blockhash string) {
	this.lock.Lock()
	defer this.lock.Unlock()

	blockBucket, ok := this.getBlockBucket(blockhash)
	if !ok {
		logger.Debug("Invalid BlockList get block %s", blockhash)
		return
	}

	blockBucket.Delete(blockhash)

	if blockBucket.Len() == 0 {
		score, err := GetBlockScore(blockhash)
		if err != nil {
			return
		}
		this.Data.Delete(score)
	}

	logger.Debug("Delete from BlockList block %s", blockhash)
}

// Range
func (this *BlockList) Range(from, to uint32) skiplist.Iterator {
	return this.Data.Range(from, to)
}
