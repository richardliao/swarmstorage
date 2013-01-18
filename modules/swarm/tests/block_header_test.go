package tests

import (
	"bytes"
	"github.com/richardliao/swarm/modules/swarm"
	"testing"
	"time"
)

func TestBlockHeader(t *testing.T) {
	now := time.Now()

	blockHeader, err := swarm.NewBlockHeader(swarm.BLOCK_TYPE_BLOCK, swarm.HEADER_STATE_NEW, now, 0, TestBlockhash)
	if err != nil {
		t.Errorf("Failed create blockheadher")
		return
	}

	headerBytes, err := swarm.DumpBlockHeader(blockHeader)
	if err != nil {
		t.Errorf("Failed dump blockheadher")
		return
	}

	newHeader, err := swarm.LoadBlockHeader(headerBytes)

	if !bytes.Equal([]byte(swarm.HEADER_VERSION), newHeader.Version) ||
		swarm.HEADER_TYPE_BLOCK != newHeader.Type ||
		swarm.HEADER_STATE_NEW != newHeader.State ||
		now != newHeader.Timestamp ||
		int64(0) != newHeader.Size ||
		!bytes.Equal(blockHeader.Checksum, newHeader.Checksum) ||
		!bytes.Equal([]byte(swarm.HEADER_TAILER), newHeader.Tailer) {
		t.Errorf("Mismatch header: %v", newHeader)
	}
}
