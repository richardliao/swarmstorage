package swarm

import (
	"fmt"
	"time"
)

// Adapted from Tideland Common Go Library - Monitoring by Frank Mueller
const (
	ssString = "Stay-Set Variable %q (%dx / act %d / min %d / max %d / avg %d)"
)

//------------------------------------------------------------------------------
// RRD: Round Robin Data structure
//------------------------------------------------------------------------------

type RRD struct {
	circle  []interface{}
	index   int
	rotated bool
}

// Add data to circle
func (this *RRD) Push(v interface{}) {
	this.circle[this.index] = v

	this.index++
	if this.index == cap(this.circle) {
		this.index = 0
		if !this.rotated {
			this.rotated = true
		}
	}
}

// Get head and tail of circle
func (this *RRD) GetHeadTail() (headv interface{}, tailv interface{}) {
	if !this.rotated {
		headv = this.circle[this.index-1]
		tailv = this.circle[0]
	} else {
		headIndex := this.index - 1
		if headIndex < 0 {
			headIndex = cap(this.circle) - 1
		}
		headv = this.circle[headIndex]
		tailv = this.circle[this.index]
	}
	return
}

//------------------------------------------------------------------------------
// SSValue
//------------------------------------------------------------------------------

// value stores a stay-set variable with a given id.
type SSValue struct {
	id    string
	value uint64
}

//------------------------------------------------------------------------------
// SSVariable
//------------------------------------------------------------------------------

// Contains the cumulated values for one stay-set variable.
type SSVariable struct {
	Count    uint64
	ActValue uint64
	MinValue uint64
	MaxValue uint64
	Total    uint64
}

// Create a new stay-set variable out of a value.
func newSSVariable(v *SSValue) *SSVariable {
	ssv := &SSVariable{
		Count:    1,
		ActValue: v.value,
		MinValue: v.value,
		MaxValue: v.value,
		Total:    v.value,
	}
	return ssv
}

// Update a ssvariable with a value.
func (this *SSVariable) UpdateValue(v *SSValue) {
	this.Count++
	this.ActValue = v.value
	this.Total += v.value

	// avoid overflow
	if this.Total < v.value {
		this.reset()
		return
	}

	if this.MinValue > this.ActValue {
		this.MinValue = this.ActValue
	}
	if this.MaxValue < this.ActValue {
		this.MaxValue = this.ActValue
	}
}

// Reset to avoid overflow
func (this *SSVariable) reset() {
	this.Count = 0
	this.ActValue = 0
	this.MinValue = 0
	this.MaxValue = 0
	this.Total = 0
}

// Implements the Stringer interface.
func (this *SSVariable) String() string {
	return fmt.Sprintf(ssString, this.Count, this.ActValue, this.MinValue, this.MaxValue)
}

//------------------------------------------------------------------------------
// AnalysisData
//------------------------------------------------------------------------------

type AnalysisData struct {
	SecondRRD  *RRD      // data of this second
	MinuteRRD  *RRD      // data of this minute
	lastMinute time.Time // last time write MinuteRRD
}

// Save round robin data
func (this *AnalysisData) UpdateRRD(ssv *SSVariable) {
	// now
	n := time.Now()

	// write SecondRRD every 0.1 second
	clone := *ssv
	this.SecondRRD.Push(&clone)

	// write MinuteRRD every second
	if n.Sub(this.lastMinute) >= 1*time.Second {
		if this.SecondRRD.index == 0 {
			return
		}
		headSecond, tailSecond := this.SecondRRD.GetHeadTail()

		ssvMinute := SSVariable{
			Count: headSecond.(*SSVariable).Count - tailSecond.(*SSVariable).Count,
			Total: headSecond.(*SSVariable).Total - tailSecond.(*SSVariable).Total,
		}

		this.MinuteRRD.Push(&ssvMinute)
		this.lastMinute = n
	}
}
