package swarm

import (
	"encoding/json"
	"io/ioutil"
	"path"
)

//------------------------------------------------------------------------------
// NodeSettings
//------------------------------------------------------------------------------

/*
Settings are runtime states of the swarm node, which is changable by admin and
should be kept even after restart until changed by admin.
*/
type NodeSettings struct {
	Devices     map[string]string // {deviceId: status}
	NodeRemoved bool              // if node removed
}

func NewNodeSettings() (this NodeSettings) {
	return NodeSettings{
		Devices:     make(map[string]string),
		NodeRemoved: false,
	}
}

// Load node setting from persistent file
// Can compatible with old version json file
// This is called only once before Start
func (this NodeSettings) Load() (err error) {
	// read
	filePath := path.Join(NODE_BASE_PATH, CONF_DIR, NODE_SETTINGS_FILE)
	if !ExistPath(filePath) {
		// file not exist, save default and return
		this.Save()
		return
	}
	encoded, err := ioutil.ReadFile(filePath)
	if err != nil {
		return
	}

	// decode
	err = json.Unmarshal(encoded, &this)
	if err != nil {
		return
	}

	return
}

// Save node setting to persistent file
// Shouled be called when settings change
func (this NodeSettings) Save() (err error) {
	// update 
	this.Devices = make(map[string]string)
	for deviceId, deviceState := range LOCAL_DEVICES.Data {
		this.Devices[deviceId] = deviceState.Status
	}

	// encode
	encoded, err := json.MarshalIndent(this, "", " ")
	if err != nil {
		return
	}

	// write
	filePath := path.Join(NODE_BASE_PATH, CONF_DIR, NODE_SETTINGS_FILE)
	err = ioutil.WriteFile(filePath, encoded, FILE_MOD)
	if err != nil {
		return
	}

	return
}
