package swarm

import (
	"time"
)

// The interface for restarter types
type Restartable interface {
	Name() string
	Start(Restartable, interface{})
}

// Restart message
type RestartMsg struct {
	r Restartable
	e interface{}
}

//------------------------------------------------------------------------------
// WorkerBase
//------------------------------------------------------------------------------

type WorkerBase struct{}

// On Panic
func (this *WorkerBase) OnPanic(r Restartable, watchdog *Watchdog, e interface{}) {
	if e != nil {
		logger.Critical(stack())
	}
	logger.Critical("%s dead accidently and will be restarted on error: %s", r.Name(), e)
	watchdog.RequestRestart(r, e)
}

//------------------------------------------------------------------------------
// Watchdog
//------------------------------------------------------------------------------

// The watchdog itself
type Watchdog struct {
	restartChan chan *RestartMsg
}

// New Watchdog
func NewWatchdog() *Watchdog {
	return &Watchdog{make(chan *RestartMsg)}
}

// Send restart request before goroutine end
func (this *Watchdog) RequestRestart(r Restartable, e interface{}) {
	this.restartChan <- &RestartMsg{r, e}
}

// Watchdog concrete function
func (this *Watchdog) Start() {
	logger.Critical("Watchdog started.")

	// monitor restart request
	for restartMsg := range this.restartChan {
		// wait some time
		time.Sleep(1e8)

		// restart dead goroutine
		logger.Critical("Restart %s recover from %s\n", restartMsg.r.Name(), restartMsg.e)
		go restartMsg.r.Start(restartMsg.r, restartMsg.e)
	}

	logger.Critical("Watchdog stopped.")
}
