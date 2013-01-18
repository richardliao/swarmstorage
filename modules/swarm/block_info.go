package swarm

import (
	"fmt"
)

//------------------------------------------------------------------------------
// BlockInfo
//------------------------------------------------------------------------------

type BlockInfo struct {
	Device string
	Type   string
	Hash   string
}

func NewBlockInfo(deviceId string, blocktype string, blockhash string) BlockInfo {
	return BlockInfo{
		Device: deviceId,
		Type:   blocktype,
		Hash:   blockhash,
	}
}

func NewBlockAnyDevice(blocktype string, blockhash string) BlockInfo {
	return NewBlockInfo(DEVICE_ID_ALL, blocktype, blockhash)
}

func NewBlockInfoFromAnyDevice(deviceId string, blockAnyDevice BlockInfo) BlockInfo {
	return NewBlockInfo(deviceId, blockAnyDevice.Type, blockAnyDevice.Hash)
}

func (this BlockInfo) String() string {
	return fmt.Sprintf("%s.%s on device %s", this.Hash, this.Type, this.Device)
}

func (this BlockInfo) Verify() bool {
	if this.Device == "" || this.Type == "" || this.Hash == "" {
		return false
	}
	return true
}
