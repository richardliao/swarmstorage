package swarm

import (
	"sync"
	"time"
)

type Semaphore struct {
	lock    *sync.Mutex
	Limit   int
	Current int
}

func NewSemaphore(max int) *Semaphore {
	return &Semaphore{
		lock:    new(sync.Mutex),
		Limit:   max,
		Current: 0,
	}
}

func (this *Semaphore) Acquire() {
	for this.Current >= this.Limit {
		time.Sleep(1 * time.Second)
	}

	this.lock.Lock()
	defer this.lock.Unlock()
	this.Current++
}

func (this *Semaphore) Release() {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.Current--
}
