package swarm

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"
)

type BlockHeader struct {
	Version   []byte    //  4 bytes: HEADER_VERSION
	Type      int8      //  1 byte : BLOCK_TYPE_BLOCK; BLOCK_TYPE_INDEX; BLOCK_TYPE_OBJECT
	State     int8      //  1 byte : HEADER_STATE_NEW; HEADER_STATE_DEL
	Timestamp time.Time //  8 bytes: CTime
	Size      int64     //  8 bytes: 0 for block; object size for object or index
	Checksum  []byte    // 20 bytes: block hash
	Tailer    []byte    //  2 bytes: HEADER_TAILER
}

// New block header
func NewBlockHeader(typeStr string, state int8, timestamp time.Time, size int64, checksumStr string) (blockHeader BlockHeader, err error) {
	var headertype int8
	var checksum []byte

	checksum, err = hex.DecodeString(checksumStr)
	if err != nil {
		return
	}
	if len(checksum) != 20 {
		err = fmt.Errorf("Invalid checksumStr %s", checksumStr)
		return
	}

	switch typeStr {
	case BLOCK_TYPE_INDEX:
		// index
		headertype = HEADER_TYPE_INDEX
	case BLOCK_TYPE_OBJECT:
		// object
		headertype = HEADER_TYPE_OBJECT
	default:
		// block
		headertype = HEADER_TYPE_BLOCK
	}

	blockHeader = BlockHeader{
		Version:   []byte(HEADER_VERSION),
		Type:      headertype,
		State:     state,
		Timestamp: timestamp,
		Size:      size,
		Checksum:  make([]byte, 20),
		Tailer:    []byte(HEADER_TAILER),
	}
	copy(blockHeader.Checksum, checksum)
	return
}

// Dump block header to bytes
func DumpBlockHeader(header BlockHeader) (headerBytes []byte, err error) {
	headerBytes = make([]byte, HEADER_SIZE)
	e1 := setBytes(&headerBytes, header.Version, 0, 4)
	e2 := setBytes(&headerBytes, header.Type, 4, 5)
	e3 := setBytes(&headerBytes, header.State, 5, 6)
	e4 := setBytes(&headerBytes, header.Timestamp.UnixNano(), 6, 14)
	e5 := setBytes(&headerBytes, header.Size, 14, 22)
	e6 := setBytes(&headerBytes, header.Checksum, 22, 42)
	e7 := setBytes(&headerBytes, header.Tailer, 42, 44)

	if string(header.Version) != HEADER_VERSION ||
		string(header.Tailer) != HEADER_TAILER ||
		e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil ||
		e6 != nil || e7 != nil {
		return nil, fmt.Errorf("Invalid block header format.")
	}
	if len(headerBytes) != HEADER_SIZE {
		err = fmt.Errorf("Failed to dump block header: %s", header)
		return
	}
	return
}

// Load block header from bytes
func LoadBlockHeader(headerBytes []byte) (header BlockHeader, err error) {
	header.Version = headerBytes[0:4]
	header.Tailer = headerBytes[42:44]
	if string(header.Version) != HEADER_VERSION || string(header.Tailer) != HEADER_TAILER {
		err = fmt.Errorf("Invalid block header format.")
		return
	}

	binary.Read(bytes.NewBuffer(headerBytes[4:5]), binary.LittleEndian, &header.Type)
	binary.Read(bytes.NewBuffer(headerBytes[5:6]), binary.LittleEndian, &header.State)

	var timestamp int64
	binary.Read(bytes.NewBuffer(headerBytes[6:14]), binary.LittleEndian, &timestamp)
	sec := timestamp / 1e9
	nsec := timestamp % 1e9
	header.Timestamp = time.Unix(sec, nsec)

	binary.Read(bytes.NewBuffer(headerBytes[14:22]), binary.LittleEndian, &header.Size)
	copyBytes(&header.Checksum, headerBytes, 22, 42)
	return
}

// Copy bytes from slice
func copyBytes(s *[]byte, headerBytes []byte, start int, end int) {
	*s = make([]byte, end-start)
	copy(*s, headerBytes[start:end])
}

// Set bytes for header field
func setBytes(headerBytes *[]byte, value interface{}, start int, end int) (err error) {
	if result, err := toBytes(value); err != nil {
		return err
	} else {
		copy((*headerBytes)[start:end], result[0:end-start])
	}
	return nil
}

// Convert any value to binary bytes using LittleEndian
func toBytes(value interface{}) (result []byte, err error) {
	buf := new(bytes.Buffer)
	if err = binary.Write(buf, binary.LittleEndian, value); err != nil {
		return
	}

	result = buf.Bytes()
	return result, nil
}
