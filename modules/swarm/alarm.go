package swarm

import (
	"fmt"
	"strconv"
	"time"
)

//------------------------------------------------------------------------------
// AlarmManager
//------------------------------------------------------------------------------

/*
Alarm is a status need be fixed by admin, and should be deleted when fixed.
*/
type Alarm struct {
	Title string
	Error string
	Type  int
	Time  time.Time
}

type AlarmManager struct {
	Alarms  map[string]Alarm  // id: Alarm
	mapping map[string]string // title: id for reverse mapping
	index   *int              // increment
}

func NewAlarmManager() AlarmManager {
	index := 0
	return AlarmManager{
		Alarms:  make(map[string]Alarm),
		mapping: make(map[string]string),
		index:   &index,
	}
}

// Add alarm
// TODO: limit total alarms
func (this AlarmManager) Add(alarmType int, err error, format string, a ...interface{}) {
	title := fmt.Sprintf(format, a...)
	if _, ok := this.mapping[title]; ok {
		// skip exist title
		return
	}
	logger.Info(title)
	this.Alarms[strconv.Itoa(*this.index)] = Alarm{title, err.Error(), alarmType, time.Now()}
	this.mapping[title] = strconv.Itoa(*this.index)
	(*this.index)++
}

// Clear specified alarm by id
func (this AlarmManager) Del(id string) (err error) {
	alarm, ok := this.Alarms[id]
	if !ok {
		return fmt.Errorf("Alarm %s not exist", id)
	}
	delete(this.Alarms, id)
	delete(this.mapping, alarm.Title)
	logger.Info("Del alarm %d: %s", id, alarm.Title)
	return nil
}

// Clear specified alarm by title
func (this AlarmManager) DelByTitle(title string) (err error) {
	id, ok := this.mapping[title]
	if !ok {
		return fmt.Errorf("Alarm %s not exist", id)
	}
	delete(this.Alarms, id)
	delete(this.mapping, title)
	logger.Info("Del alarm %d: %s", id, title)
	return nil
}
