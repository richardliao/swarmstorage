package swarm

import (
	"fmt"
	"time"
)

const (
	cmdSSVariableRead = iota
	cmdAnalysisDataUpdate
)

// Command encapsulated the data for any command.
type MonitorCommand struct {
	opCode   int
	args     interface{}
	respChan chan interface{}
}

//------------------------------------------------------------------------------
// Monitor
//------------------------------------------------------------------------------

/*
Gather measurable data from swarm workers and save in ssData, reflecting current 
system status. A worker trigger to analyse these data periodically, save them to
round robin data in analysisData.
With these analyzed data, we now have the ability to get healthy state of the
node, and take appropriate measures of further actions.
MonitorServer serve raw monitor data for outer interestings, such as domain
monitoring, admin tools etc.
*/
type Monitor struct {
	WorkerBase
	watchdog     *Watchdog
	ssData       map[string]*SSVariable   // accumulated raw data gathered from workers
	analysisData map[string]*AnalysisData // analyzed round robin data
	valueChan    chan *SSValue
	commandChan  chan *MonitorCommand
}

// Implement interface Restartable
func (this *Monitor) Name() string {
	return "Monitor"
}

// Implement interface Restartable
func (this *Monitor) Start(r Restartable, e interface{}) {
	defer func() {
		e := recover()
		this.OnPanic(this, this.watchdog, e)
	}()

	logger.Critical("Monitor started.")

	// TODO: calc data will be wrong when in rigorous time
	go this.Poll()

	// handle chan
	for {
		select {
		case value := <-this.valueChan:
			// received a new value.
			if ssv, ok := this.ssData[value.id]; ok {
				// variable found.
				ssv.UpdateValue(value)
			} else {
				// new stay-set variable.
				this.ssData[value.id] = newSSVariable(value)
			}
		case cmd := <-this.commandChan:
			// receivedd a command to process.
			this.processCommand(cmd)
		}
	}
}

// Process a command.
func (this *Monitor) processCommand(cmd *MonitorCommand) {
	switch cmd.opCode {
	case cmdSSVariableRead:
		// Read just one stay-set variable.
		id := cmd.args.(string)
		if ssv, ok := this.ssData[id]; ok {
			// Variable found.
			clone := *ssv
			cmd.respChan <- &clone
		} else {
			// Variable does not exist.
			cmd.respChan <- fmt.Errorf("stay-set variable %q does not exist", id)
		}
	case cmdAnalysisDataUpdate:
		// Update analysis data
		for id, ssv := range MONITOR.ssData {
			if analysisData, ok := MONITOR.analysisData[id]; ok {
				analysisData.UpdateRRD(ssv)
			} else {

				MONITOR.analysisData[id] = &AnalysisData{
					SecondRRD: &RRD{
						circle:  make([]interface{}, 10, 10),
						index:   0,
						rotated: false,
					},
					lastMinute: time.Now(),
					MinuteRRD: &RRD{
						circle:  make([]interface{}, 60, 60),
						index:   0,
						rotated: false,
					},
				}
			}
		}
	}
}

// Sets a value of a stay-set variable.
func (this *Monitor) SetVariable(id string, v interface{}) {
	v1, err := toUint64(v)
	if err != nil {
		return
	}

	this.valueChan <- &SSValue{id, v1}
}

// Returns the stay-set variable for an id.
func (this *Monitor) ReadVariable(id string) (*SSVariable, error) {
	cmd := &MonitorCommand{cmdSSVariableRead, id, make(chan interface{})}
	this.commandChan <- cmd
	resp := <-cmd.respChan
	if _, ok := resp.(error); ok {
		value := &SSValue{id: id, value: 0}
		return newSSVariable(value), nil
	}
	return resp.(*SSVariable), nil
}

// Do analysis every 0.1s
func (this *Monitor) Poll() {
	for {
		// set stat
		setCpustats()
		setDiskstats()
		setNetstats(NODE_ADDRESS)
		if NODE_ADDRESS != OSS_ADDRESS {
			setNetstats(OSS_ADDRESS)
		}

		// push cmdAnalysisDataUpdate
		this.commandChan <- &MonitorCommand{cmdAnalysisDataUpdate, nil, make(chan interface{})}
		time.Sleep(1e8)
	}
}

// Get cpu busy stats in second
func (this *Monitor) GetCpuBusySecond() (cpuBusy uint64) {
	busy := this.getSecond("S:CPU:UserTime") + this.getSecond("S:CPU:NiceTime") + this.getSecond("S:CPU:SysTime")
	total := busy + this.getSecond("S:CPU:IdleTime")
	if total == 0 {
		return 100
	}
	cpuBusy = busy * 100 / total
	return
}

// Get disk busy stats in second
func (this *Monitor) GetDiskBusySecond(deviceId string) (diskBusy uint64) {
	id := fmt.Sprintf("S:DISK:%s:IoTime", deviceId)

	total := this.getSecond("S:CPU:UserTime") + this.getSecond("S:CPU:NiceTime") + this.getSecond("S:CPU:SysTime") + this.getSecond("S:CPU:IdleTime")
	deltaMs := total * uint64(1000/NCPU/HZ)
	if deltaMs == 0 {
		return 100
	}

	diskBusy = this.getSecond(id) * 100 / deltaMs
	return
}

// Get stats of second
func (this *Monitor) getSecond(id string) (v uint64) {
	analysisData, ok := this.analysisData[id]
	if !ok {
		return
	}

	if analysisData.SecondRRD.index == 0 {
		return
	}

	head, tail := analysisData.SecondRRD.GetHeadTail()
	v = head.(*SSVariable).Total - tail.(*SSVariable).Total
	return
}
