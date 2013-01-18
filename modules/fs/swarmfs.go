package fs

import (
	"fmt"
	//    "bytes"
	//    "encoding/binary"
	//    "os"
	"github.com/richardliao/swarm/modules/swarm"
	"github.com/richardliao/swarm/third_party/github.com/hanwen/go-fuse/fuse"
	"syscall"
)

// TODO: swarmServers

/*
Write file:

1. if file exists:
FS GetAttr 6c86f5285d7e6254a9d4a9b75c75966c7d31dc60
Truncate 6c86f5285d7e6254a9d4a9b75c75966c7d31dc60
FS GetAttr 6c86f5285d7e6254a9d4a9b75c75966c7d31dc60
Open 6c86f5285d7e6254a9d4a9b75c75966c7d31dc60 flags 0x8001
Write
Release

2. if file not exists:
FS GetAttr 6c86f5285d7e6254a9d4a9b75c75966c7d31dc61
404 Not Found
Create 6c86f5285d7e6254a9d4a9b75c75966c7d31dc61 flags 0x8241 mode 0x81a4
FS GetAttr 6c86f5285d7e6254a9d4a9b75c75966c7d31dc61
404 Not Found
Write
Release

*/

// Global
var logger swarm.Logger

//------------------------------------------------------------------------------
// SwarmFS delegates all operations back to swarm OSS.
//------------------------------------------------------------------------------
type SwarmFS struct {
	SwarmServers string

	fuse.DefaultFileSystem
}

func NewSwarmFS(swarmServers string, log_file string) (swarmfs *SwarmFS) {
	swarmfs = new(SwarmFS)
	swarmfs.SwarmServers = swarmServers

	// init log
	logger = swarm.InitLog(log_file)
	logger.Critical("Swarmfs started.")

	return swarmfs
}

func (this *SwarmFS) String() string {
	return fmt.Sprintf("SwarmFS(%s)", this.SwarmServers)
}

func (this *SwarmFS) Open(name string, flags uint32, context *fuse.Context) (fuseFile fuse.File, status fuse.Status) {
	logger.Debug("Open %s flags %#v", name, flags)
	return &SwarmObject{Objecthash: name, SwarmServers: this.SwarmServers}, fuse.OK
}

func (this *SwarmFS) Create(name string, flags uint32, mode uint32, context *fuse.Context) (fuseFile fuse.File, code fuse.Status) {
	logger.Debug("Create %s flags %#v mode %#v", name, flags, mode)
	return &SwarmObject{Objecthash: name, SwarmServers: this.SwarmServers}, fuse.OK
}

func (this *SwarmFS) Truncate(name string, offset uint64, context *fuse.Context) (code fuse.Status) {
	logger.Debug("Truncate %s", name)

	return fuse.OK
}

func (this *SwarmFS) GetAttr(name string, context *fuse.Context) (a *fuse.Attr, code fuse.Status) {
	if len(name) != swarm.HASH_HEX_SIZE {
		// root mount point, just return ENOENT
		// return OK will cause Input/output error, then no more ls success
		return nil, fuse.ENOENT
	}
	logger.Debug("FS GetAttr %s", name)

	a, err := setAttr(name, this.SwarmServers)
	if err != nil {
		// assume any error to no entry
		a = newAttr(int64(0), uint64(0))
		return a, fuse.ENOENT
	}
	return a, fuse.OK
}

func (this *SwarmFS) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	logger.Debug("Unlink %s", name)

	return fuse.OK
}

// Just to make df happy
// Show stat of root
func (this *SwarmFS) StatFs(name string) *fuse.StatfsOut {
	if len(name) != swarm.HASH_HEX_SIZE {
		return nil
	}

	logger.Debug("StatFs %s", name)

	s := syscall.Statfs_t{}
	err := syscall.Statfs("/", &s)
	if err == nil {
		return &fuse.StatfsOut{
			Blocks:  s.Blocks,
			Bsize:   uint32(s.Bsize),
			Bfree:   s.Bfree,
			Bavail:  s.Bavail,
			Files:   s.Files,
			Ffree:   s.Ffree,
			Frsize:  uint32(s.Frsize),
			NameLen: uint32(s.Namelen),
		}
	}
	return nil
}

func (this *SwarmFS) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
	logger.Debug("OpenDir %s", name)
	output := make([]fuse.DirEntry, 0)
	return output, fuse.OK
}

// not implemented

//func (this *SwarmFS) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
//    logger.Debug("OpenDir %s", name)
//    return nil, fuse.OK
//}

//func (this *SwarmFS) Utimens(path string, AtimeNs int64, MtimeNs int64, context *fuse.Context) (code fuse.Status) {
//    logger.Debug("Utimens %s", path)
//    return fuse.OK
//}

//func (this *SwarmFS) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
//    logger.Debug("Access %s", name)
//    return fuse.OK
//}

//func (this *SwarmFS) GetXAttr(name string, attr string, context *fuse.Context) ([]byte, fuse.Status) {
//    logger.Debug("GetXAttr %s", name)
//    return nil, fuse.OK
//}

//func (this *SwarmFS) ListXAttr(name string, context *fuse.Context) ([]string, fuse.Status) {
//    logger.Debug("ListXAttr %s", name)
//    return nil, fuse.OK
//}

//func (this *SwarmFS) Link(orig string, newName string, context *fuse.Context) (code fuse.Status) {
//    logger.Debug("Link %s", orig)
//    return fuse.OK
//}

//func (this *SwarmFS) Symlink(pointedTo string, linkName string, context *fuse.Context) (code fuse.Status) {
//    logger.Debug("Symlink %s", pointedTo)
//    return fuse.OK
//}

//func (this *SwarmFS) Rename(oldPath string, newPath string, context *fuse.Context) (code fuse.Status) {
//    logger.Debug("Rename %s", oldPath)
//    return fuse.OK
//}

//func (this *SwarmFS) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
//    logger.Debug("Rmdir %s", name)
//    return fuse.OK
//}

//func (this *SwarmFS) Readlink(name string, context *fuse.Context) (out string, code fuse.Status) {
//    logger.Debug("Readlink %s", name)
//    return out, fuse.OK
//}

//func (this *SwarmFS) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) (code fuse.Status) {
//    logger.Debug("Mknod %s", name)
//    return fuse.OK
//}

//func (this *SwarmFS) Mkdir(path string, mode uint32, context *fuse.Context) (code fuse.Status) {
//    logger.Debug("Mkdir %s", path)
//    return fuse.OK
//}
//func (this *SwarmFS) Chmod(path string, mode uint32, context *fuse.Context) (code fuse.Status) {
//    logger.Debug("Chmod %s", path)
//    return fuse.OK
//}

//func (this *SwarmFS) Chown(path string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
//    logger.Debug("Chown %s", path)
//    return fuse.OK
//}
