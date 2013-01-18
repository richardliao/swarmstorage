package swarm

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
)

// TODO: https://github.com/bpowers/psm/blob/master/proc_linux.go

//------------------------------------------------------------------------------
// BufferedReader
//------------------------------------------------------------------------------

type BufferedReader struct {
	*bufio.Reader
}

func NewBufferedReader(r io.Reader) *BufferedReader {
	return &BufferedReader{
		bufio.NewReader(r),
	}
}

// Read whole line from buffer
func (b *BufferedReader) ReadWholeLine() (line string, err error) {
	byteline := make([]byte, 0)
	prefix := true
	for prefix {
		var partial []byte
		partial, prefix, err = b.Reader.ReadLine()
		if err != nil {
			break
		}
		byteline = append(byteline, partial...)
	}
	return string(byteline), err
}

//------------------------------------------------------------------------------
// Process
//------------------------------------------------------------------------------

type Process struct {
	*os.Process
	Name     string
	Owner    int
	Cmdline  string
	State    string
	Parent   int
	MemUsage int
}

// Returns a slice of all pids currently existing
func GetAllPids() (r []int, err error) {
	r = make([]int, 0)

	f, err := os.Open("/proc")
	if err != nil {
		err = fmt.Errorf("Could not open /proc")
		return nil, err
	}

	elems, err := f.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	for _, elem := range elems {
		if IsNumeric(elem) {
			pid := toInt(elem)
			r = append(r, pid)
		}
	}
	return r, nil
}

// Get process by pid
func GetProcessByPid(pid int) (p *Process, err error) {
	p = &Process{}
	p.Process, err = os.FindProcess(pid)
	if err != nil {
		return nil, err
	}

	procstats, err := parseProcStats(pid)
	if err != nil {
		return nil, err
	}
	p.Name = procstats["Name"]
	p.Owner = getOwnerID(procstats["Uid"])
	p.State = procstats["State"]
	p.Parent = toInt(procstats["PPid"])
	p.MemUsage = getMemUsage(procstats["VmSize"])
	p.Cmdline, err = getCmdlineByPid(pid)
	if err != nil {
		return nil, err
	}
	return p, nil

}

// Read process status file
func parseProcStats(pid int) (procstats map[string]string, err error) {
	procstats = make(map[string]string)

	filename := fmt.Sprintf("/proc/%d/status", pid)
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := NewBufferedReader(f)
	for l, err := r.ReadWholeLine(); err == nil; {
		parts := strings.SplitN(l, ":", 2)
		procstats[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		l, err = r.ReadWholeLine()
	}
	if err == io.EOF {
		err = nil
	}
	return procstats, err
}

// Get owner uid
func getOwnerID(uid string) int {
	parts := strings.Split(uid, "\t")
	nuid := toInt(parts[0])
	return nuid
}

// Get command line
func getCmdlineByPid(pid int) (l string, err error) {
	filename := fmt.Sprintf("/proc/%d/cmdline", pid)
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close()

	r := NewBufferedReader(f)
	l, err = r.ReadWholeLine()
	if err != nil && err != io.EOF {
		return "", err
	}

	return l, nil
}

// Get mem usage
func getMemUsage(vmsize string) (nvmsize int) {
	fmt.Sscanf(vmsize, "%d kB", &nvmsize)
	return
}

//------------------------------------------------------------------------------
// helper
//------------------------------------------------------------------------------

// Set monitor variable with total value
func setWithTotal(id string, totalCur int) {
	ssvPre, ok := MONITOR.ssData[id]
	if !ok {
		MONITOR.SetVariable(id, 0)
		return
	}
	MONITOR.SetVariable(id, (uint64(totalCur) - ssvPre.Total))
}

//------------------------------------------------------------------------------
// CPU
//------------------------------------------------------------------------------

// TODO: use sysconf(_SC_CLK_TCK) to get HZ
const HZ = 100

type Cpustats struct {
	UserTime int
	NiceTime int
	SysTime  int
	IdleTime int
}

// Parse /proc/stat
func parseCPUStat() (cpustats Cpustats, err error) {
	filename := fmt.Sprintf("/proc/stat")
	f, err := os.Open(filename)
	if err != nil {
		return
	}
	defer f.Close()

	r := NewBufferedReader(f)
	l, err := r.ReadWholeLine()
	if err != nil && err != io.EOF {
		return
	}

	// cpu  1383764 74321 544751 17699291 99587 4226 4209 0 0
	if !strings.HasPrefix(l, "cpu") {
		err = fmt.Errorf("Failed to get cpu stat")
		return
	}

	parts := strings.Fields(l)
	cpustats.UserTime = toInt(strings.TrimSpace(parts[1]))
	cpustats.NiceTime = toInt(strings.TrimSpace(parts[2]))
	cpustats.SysTime = toInt(strings.TrimSpace(parts[3]))
	cpustats.IdleTime = toInt(strings.TrimSpace(parts[4]))

	return cpustats, nil
}

// Set cpu stats
func setCpustats() {
	cpustats, err := parseCPUStat()
	if err != nil {
		return
	}

	setWithTotal("S:CPU:UserTime", cpustats.UserTime)
	setWithTotal("S:CPU:NiceTime", cpustats.NiceTime)
	setWithTotal("S:CPU:SysTime", cpustats.SysTime)
	setWithTotal("S:CPU:IdleTime", cpustats.IdleTime)
}

//------------------------------------------------------------------------------
// Diskstats
//------------------------------------------------------------------------------

type Diskstats struct {
	Reads             int
	ReadsMerged       int
	ReadKbytesPerSec  int
	ReadTime          int
	Writes            int
	WritesMerged      int
	WriteKbytesPerSec int
	WriteTime         int
	Other             int
	IoTime            int
	WeightedIoTime    int
}

// Parse /proc/diskstats
func parseDiskstats() (diskstats map[string]Diskstats, err error) {
	diskstats = make(map[string]Diskstats)

	filename := fmt.Sprintf("/proc/diskstats")
	f, err := os.Open(filename)
	if err != nil {
		return
	}
	defer f.Close()

	r := NewBufferedReader(f)
	for l, err := r.ReadWholeLine(); err == nil; {
		parts := strings.Fields(l)

		k := strings.TrimSpace(parts[2])
		if strings.HasPrefix(k, "ram") || strings.HasPrefix(k, "loop") {
			l, err = r.ReadWholeLine()
			continue
		}

		diskstats[k] = Diskstats{
			Reads:             toInt(parts[3]),
			ReadsMerged:       toInt(parts[4]),
			ReadKbytesPerSec:  toInt(parts[5]),
			ReadTime:          toInt(parts[6]),
			Writes:            toInt(parts[7]),
			WritesMerged:      toInt(parts[8]),
			WriteKbytesPerSec: toInt(parts[9]),
			WriteTime:         toInt(parts[10]),
			Other:             toInt(parts[11]),
			IoTime:            toInt(parts[12]),
			WeightedIoTime:    toInt(parts[13]),
		}
		l, err = r.ReadWholeLine()
	}
	if err == io.EOF {
		err = nil
	}
	return
}

// Parse /proc/mounts
func parseMounts() (mounts map[string]string, err error) {
	// {"/": rootfs, "/data": "/dev/sda7", }
	mounts = make(map[string]string)

	filename := fmt.Sprintf("/proc/mounts")
	f, err := os.Open(filename)
	if err != nil {
		return
	}
	defer f.Close()

	r := NewBufferedReader(f)
	for l, err := r.ReadWholeLine(); err == nil; {
		parts := strings.Fields(l)

		v := strings.TrimSpace(parts[0])
		//if !strings.HasPrefix(v, "/") {
		//	l, err = r.ReadWholeLine()
		//    continue
		//}

		mounts[strings.TrimSpace(parts[1])] = v
		l, err = r.ReadWholeLine()
	}
	if err == io.EOF {
		err = nil
	}
	return
}

// Find mount point of device path
func matchMountPoint(devicePath string, mounts map[string]string) (mountPoint string) {
	for m := range mounts {
		if strings.HasPrefix(devicePath, m) {
			if len(mountPoint) < len(m) {
				mountPoint = m
			}
		}
	}
	return
}

// Set disk stats
func setDiskstats() {
	// parse mounts
	mounts, err := parseMounts()
	if err != nil {
		return
	}

	// all local devices
	for deviceId, deviceState := range LOCAL_DEVICES.Data {
		// get devname
		mountPoint := matchMountPoint(deviceState.Path, mounts)
		devPath, ok := mounts[mountPoint]
		if !ok {
			return
		}

		parts := strings.Split(devPath, "/")
		devname := parts[len(parts)-1]

		// get diskstats
		diskstats, err := parseDiskstats()
		if err != nil {
			return
		}

		diskstat, ok := diskstats[devname]
		if !ok {
			return
		}

		setWithTotal(fmt.Sprintf("S:DISK:%s:IoTime", deviceId), diskstat.IoTime)
	}
}

//------------------------------------------------------------------------------
// Netstats
//------------------------------------------------------------------------------

type Netstats struct {
	ReceiveBytes      int
	ReceivePackages   int
	ReceiveMulticast  int
	TransmitBytes     int
	TransmitPackages  int
	TransmitMulticast int
}

//  face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
// Parse /proc/net/dev
func parseNetdev() (netstats map[string]Netstats, err error) {
	netstats = make(map[string]Netstats)

	filename := fmt.Sprintf("/proc/net/dev")
	f, err := os.Open(filename)
	if err != nil {
		return
	}
	defer f.Close()

	r := NewBufferedReader(f)
	for l, err := r.ReadWholeLine(); err == nil; {
		parts := strings.SplitN(l, ":", 2)
		if len(parts) != 2 {
			l, err = r.ReadWholeLine()
			continue
		}
		k := strings.TrimSpace(parts[0])

		parts = strings.Fields(parts[1])
		netstats[k] = Netstats{
			ReceiveBytes:      toInt(parts[0]),
			ReceivePackages:   toInt(parts[1]),
			ReceiveMulticast:  toInt(parts[7]),
			TransmitBytes:     toInt(parts[8]),
			TransmitPackages:  toInt(parts[9]),
			TransmitMulticast: toInt(parts[15]),
		}
		l, err = r.ReadWholeLine()
	}
	if err == io.EOF {
		err = nil
	}
	return
}

// Get ip of IPNet addr
func getAddrIp(addr interface{}) (ip string, err error) {
	err = fmt.Errorf("Invalid ip")

	switch addr := addr.(type) {
	case *net.IPNet:
		ip = addr.IP.String()
		return ip, nil
	default:
		return
	}
	return
}

// Get interface name
func getIfname(targetIp string) (ifname string) {
	ifs, _ := net.Interfaces()
	for _, ifat := range ifs {
		//fmt.Printf("ifs: %#v\n", ifat.Name)
		addrs, _ := ifat.Addrs()
		for _, addr := range addrs {
			ip, err := getAddrIp(addr)
			if err != nil {
				continue
			}
			// match ip
			if ip == targetIp {
				//fmt.Printf("found ip %s on %s\n", ip, ifat.Name)
				return ifat.Name
			}
		}
	}
	return
}

// Set net stats
func setNetstats(targetIp string) {
	// get ifname
	ifname := getIfname(targetIp)

	// get netstats
	netstats, err := parseNetdev()
	if err != nil {
		return
	}

	netstat, ok := netstats[ifname]
	if !ok {
		return
	}

	setWithTotal(fmt.Sprintf("S:NET:%s:ReceiveBytes", targetIp), netstat.ReceiveBytes)
	setWithTotal(fmt.Sprintf("S:NET:%s:ReceivePackages", targetIp), netstat.ReceivePackages)
	setWithTotal(fmt.Sprintf("S:NET:%s:ReceiveMulticast", targetIp), netstat.ReceiveMulticast)
	setWithTotal(fmt.Sprintf("S:NET:%s:TransmitBytes", targetIp), netstat.TransmitBytes)
	setWithTotal(fmt.Sprintf("S:NET:%s:TransmitPackages", targetIp), netstat.TransmitPackages)
	setWithTotal(fmt.Sprintf("S:NET:%s:TransmitMulticast", targetIp), netstat.TransmitMulticast)
}

//------------------------------------------------------------------------------
// syscall
//------------------------------------------------------------------------------

// Get sysinfo
func GetSysinfo() (sysinfo *syscall.Sysinfo_t, err error) {
	sysinfo = new(syscall.Sysinfo_t)

	err = syscall.Sysinfo(sysinfo)
	if err != nil {
		ALARM_MANAGER.Add(ALARM_SYSCALL, err, "Sysinfo failed")
		logger.Debug("Failed in syscall sysinfo, error: %s", err.Error())
		return
	}
	return sysinfo, nil
}

// Get disk space info
func GetDiskStatfs(deviceId string) (statfs *syscall.Statfs_t, err error) {
	statfs = new(syscall.Statfs_t)

	devicePath, ok := LOCAL_DEVICES.LocalDevicePath(deviceId)
	if !ok {
		err = fmt.Errorf("Invalid device %s", deviceId)
		return
	}

	err = syscall.Statfs(devicePath, statfs)
	if err != nil {
		ALARM_MANAGER.Add(ALARM_SYSCALL, err, "Statfs failed")
		logger.Debug("Failed in syscall Statfs, error: %s", err.Error())
		return
	}
	return statfs, nil
}

// Get rusage
func GetRusage() (rusage *syscall.Rusage, err error) {
	rusage = new(syscall.Rusage)

	err = syscall.Getrusage(syscall.RUSAGE_SELF, rusage)
	if err != nil {
		ALARM_MANAGER.Add(ALARM_SYSCALL, err, "Getrusage failed")
		logger.Debug("Failed in syscall Getrusage, error: %s", err.Error())
		return
	}
	return rusage, nil
}

////------------------------------------------------------------------------------
//// test
////------------------------------------------------------------------------------

//func test() {
//    if runtime.GOOS != "linux" {
//        return
//    }

//    // get process
//    p, _ := GetProcessByPid(os.Getpid())
//    fmt.Printf("Process: %#v\n", p)

//    // get ifname
//    targetIp := "192.168.200.1"
//    ifname := getIfname(targetIp)

//    // get devname
//    mounts, _ := parseMounts()
//    fmt.Printf("parseMounts: %#v\n", mounts)

//    var busy, total, ioTime int
//    mountPoint := "/data"
//    devPath, ok := mounts[mountPoint]
//    if !ok {
//        return
//    }
//    parts := strings.Split(devPath, "/")
//    devname := parts[len(parts)-1]

//    ncpu := runtime.NumCPU()

//    for {
//        // cpu stat
//        cpustats, _ := parseCPUStat()
//        busy1 := cpustats.UserTime + cpustats.NiceTime + cpustats.SysTime
//        total1 := busy1 + cpustats.IdleTime
//        deltaMs := (total1 - total) * (1000 / ncpu / HZ) 

//        fmt.Printf("cpu: %d\n", (busy1 - busy) * 100 / (total1 - total))
//        busy, total = busy1, total1

//        // disk stat
//        diskstats, _ := parseDiskstats()
//        diskstat, ok := diskstats[devname]
//        if !ok {
//            time.Sleep(1e9)
//            continue
//        }
//        ticks := diskstat.IoTime - ioTime
//        ioTime = diskstat.IoTime

//        busy := 100 * ticks / deltaMs
//        fmt.Printf("diskbusy %s: %#v\n", devname, busy)

//        // net stat
//        netstats, _ := parseNetdev()
//        netstat, ok := netstats[ifname]
//        if !ok {
//            time.Sleep(1e9)
//            continue
//        }
//        fmt.Printf("netstat %s: %#v\n", ifname, netstat.ReceiveBytes)

//        time.Sleep(1e9)
//    }
//}

////------------------------------------------------------------------------------
//// Memory
////------------------------------------------------------------------------------

//type Memory struct {
//	MemTotal int
//	MemFree int
//	Buffers int
//	Cached int
//	SwapTotal int
//	SwapFree int
//}

//// Get process
//func GetMeminfo() (m *Memory, err error) {
//	meminfo, err := parseMeminfo()
//	if err != nil {
//		return nil, err
//	}

//	m = &Memory{}
//	m.MemTotal = getMemUsage(meminfo["MemTotal"])
//	m.MemFree = getMemUsage(meminfo["MemFree"])
//	m.Buffers = getMemUsage(meminfo["Buffers"])
//	m.Cached = getMemUsage(meminfo["Cached"])
//	m.SwapTotal = getMemUsage(meminfo["SwapTotal"])
//	m.SwapFree = getMemUsage(meminfo["SwapFree"])
//	return m, nil

//}

//// Read meminfo file
//func parseMeminfo() (meminfo map[string]string, err error) {
//	meminfo = make(map[string]string)

//	filename := fmt.Sprintf("/proc/meminfo")
//	f, err := os.Open(filename)
//	if err != nil {
//		return nil, err
//	}
//	defer f.Close()

//	r := NewBufferedReader(f)
//	for l, err := r.ReadWholeLine(); err == nil; {
//		parts := strings.SplitN(l, ":", 2)
//		meminfo[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
//		l, err = r.ReadWholeLine()
//	}
//	if err == io.EOF {
//		err = nil
//	}
//	return meminfo, err
//}
