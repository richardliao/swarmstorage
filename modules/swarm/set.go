package swarm

import (
	"sort"
	"sync"
)

//------------------------------------------------------------------------------
// StringSet
//------------------------------------------------------------------------------

type StringSet struct {
	lock *sync.RWMutex
	Data map[string]interface{}
}

func NewStringSet() (this *StringSet) {
	this = &StringSet{
		lock: new(sync.RWMutex),
		Data: make(map[string]interface{}),
	}
	return
}

func (this *StringSet) Contains(item string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()

	if _, ok := this.Data[item]; ok {
		return true
	}
	return false
}

func (this *StringSet) Add(item string) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.Data[item] = nil
}

func (this *StringSet) Delete(item string) {
	this.lock.Lock()
	defer this.lock.Unlock()

	delete(this.Data, item)
}

func (this *StringSet) Len() int {
	this.lock.RLock()
	defer this.lock.RUnlock()

	return len(this.Data)
}

func (this *StringSet) Sorted() (items []string) {
	items = make([]string, 0)
	for item := range this.Data {
		items = append(items, item)
	}
	sort.Strings(items)
	return
}

//------------------------------------------------------------------------------
// ScoreSegSet
//------------------------------------------------------------------------------

type ScoreSegSet struct {
	lock *sync.RWMutex
	Data map[ScoreSeg]interface{}
}

func NewScoreSegSet() (this *ScoreSegSet) {
	this = &ScoreSegSet{
		lock: new(sync.RWMutex),
		Data: make(map[ScoreSeg]interface{}),
	}
	return
}

func (this *ScoreSegSet) Contains(item ScoreSeg) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()

	if _, ok := this.Data[item]; ok {
		return true
	}
	return false
}

func (this *ScoreSegSet) Add(item ScoreSeg) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.Data[item] = nil
}

func (this *ScoreSegSet) Delete(item ScoreSeg) {
	this.lock.Lock()
	defer this.lock.Unlock()

	delete(this.Data, item)
}

func (this *ScoreSegSet) Len() int {
	this.lock.RLock()
	defer this.lock.RUnlock()

	return len(this.Data)
}

//------------------------------------------------------------------------------
// Uint32Set
//------------------------------------------------------------------------------

type Uint32Set struct {
	lock *sync.RWMutex
	Data map[uint32]interface{}
}

func NewUint32Set() (this *Uint32Set) {
	this = &Uint32Set{
		lock: new(sync.RWMutex),
		Data: make(map[uint32]interface{}),
	}
	return
}

func (this *Uint32Set) Contains(item uint32) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()

	if _, ok := this.Data[item]; ok {
		return true
	}
	return false
}

func (this *Uint32Set) Add(item uint32) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.Data[item] = nil
}

func (this *Uint32Set) Delete(item uint32) {
	this.lock.Lock()
	defer this.lock.Unlock()

	delete(this.Data, item)
}

func (this *Uint32Set) Len() int {
	this.lock.RLock()
	defer this.lock.RUnlock()

	return len(this.Data)
}
