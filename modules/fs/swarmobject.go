package fs

import (
	"bytes"
	"fmt"
	"github.com/richardliao/swarm/third_party/github.com/hanwen/go-fuse/fuse"
)

//------------------------------------------------------------------------------
// SwarmObject delegates all operations back to an swarm object.
//------------------------------------------------------------------------------
type SwarmObject struct {
	Objecthash   string
	SwarmServers string
	FileBuffer   bytes.Buffer
	BufferSize   int64

	fuse.DefaultFile
}

func (this *SwarmObject) String() string {
	return fmt.Sprintf("SwarmObject(%s)", this.Objecthash)
}

func (this *SwarmObject) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	logger.Debug("Read %s offset %d to buffer, size %d", this.Objecthash, off, len(buf))
	body, err := readObject(this.Objecthash, this.SwarmServers, off)
	if err != nil {
		return &fuse.ReadResultData{}, fuse.ToStatus(err)
	}

	end := int(off) + int(len(buf))
	if end > len(body) {
		end = len(body)
	}
	return &fuse.ReadResultData{body[off:end]}, fuse.OK
}

func (this *SwarmObject) Write(data []byte, off int64) (uint32, fuse.Status) {
	logger.Debug("Write %s %d bytes, offset %d", this.Objecthash, len(data), off)
	// buffer to FileBuffer
	if off != this.BufferSize {
		logger.Debug("Invalid write file offset %d, current buffer %d", off, this.BufferSize)
		return uint32(0), fuse.EINVAL
	}
	n, err := this.FileBuffer.Write(data)
	if err != nil {
		logger.Debug("Invalid write file offset %d", off)
		return uint32(0), fuse.EINVAL
	}
	this.BufferSize += int64(n)

	// TODO: check end of file by objecthash
	if len(data) == 32768 {
		return uint32(len(data)), fuse.OK
	}
	if err := writeObject(this.Objecthash, this.FileBuffer.Bytes(), this.SwarmServers); err != nil {
		logger.Debug("Error: %s", err)
		return uint32(0), fuse.EINVAL
	}

	return uint32(len(data)), fuse.OK
}

func (this *SwarmObject) GetAttr(out *fuse.Attr) fuse.Status {
	logger.Debug("File GetAttr %s", this.Objecthash)

	a, err := setAttr(this.Objecthash, this.SwarmServers)
	if err != nil {
		logger.Debug("File GetAttr error %s", err)
		return fuse.ENOENT
	}

	// copy to out
	*out = *a

	return fuse.OK
}

func (this *SwarmObject) Release() {
	logger.Debug("Release %s", this.Objecthash)

	//this.Objecthash.Close()
}

// not implemented

//func (this *SwarmObject) Flush() fuse.Status {
//    logger.Debug("File Flush")
//    return fuse.OK
//}

//func (this *SwarmObject) Fsync(flags int) (code fuse.Status) {
//    logger.Debug("File Fsync")
//    return fuse.OK
//}

//func (this *SwarmObject) Truncate(size uint64) fuse.Status {
//    logger.Debug("File Truncate")
//    return fuse.OK
//}

//func (this *SwarmObject) Chmod(mode uint32) fuse.Status {
//    logger.Debug("File Chmod")
//    return fuse.OK
//}

//func (this *SwarmObject) Chown(uid uint32, gid uint32) fuse.Status {
//    logger.Debug("File Chown")
//    return fuse.OK
//}
