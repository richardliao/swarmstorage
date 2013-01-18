package swarm

import (
	"bufio"
	crand "crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"
)

// Global
var NCPU int

func init() {
	NCPU = runtime.NumCPU()
}

// Rand interval
func randInterval(interval time.Duration) time.Duration {
	return time.Duration(rand.Int63n(int64(interval))) + interval
}

// A network helper.  Read from the wire until a 0x00 byte is encountered.
func readUntilNull(r *bufio.Reader) (b []byte, err error) {
	b, err = r.ReadBytes(0)
	if err != nil {
		return b, err
	}
	if len(b) == 1 {
		b = make([]byte, 0)
	} else {
		b = b[0 : len(b)-1]
	}
	return b, err
}

// A network helper.  Read a full message body with a known length that is > 0.
func readBody(r *bufio.Reader, l int64) (b []byte, err error) {
	b = make([]byte, l)
	if l == 0 {
		return b, nil
	}
	n, err := io.ReadFull(r, b)
	if int64(n) < l { // Short read, err is ErrUnexpectedEOF
		return b[0:n], err
	}
	if err != nil { // Other erors
		return b, err
	}
	return b, err
}

// Return a UUID.
func Uuid() string {
	b := make([]byte, 16)
	_, _ = io.ReadFull(crand.Reader, b)
	b[6] = (b[6] & 0x0F) | 0x40
	b[8] = (b[8] &^ 0x40) | 0x80
	return hex.EncodeToString(b)
}

// Not used
func getAllMacs() (result string, err error) {
	result = ""
	err = nil
	//
	ifaces, err := net.Interfaces()
	if err != nil {
		return
	}
	for _, face := range ifaces {
		//
		result += (face.HardwareAddr.String() + "~")
	}
	//
	return
}

// Check if path exists in local fs
func ExistPath(name string) (exist bool) {
	fi, err := os.Stat(name)
	if err == nil && fi != nil {
		return true
	}
	return false
	//return os.IsExist(err)

	//// check if error is "no such file or directory"
	//if e, ok := err.(*os.PathError); ok && e.Error == os.ENOENT {
	//    exist = false
	//    return
	//}
	//exist = false
	//return
}

// used in nodeinit
func CreateFile(name string) (f *os.File, err error) {
	f, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if os.IsExist(err) {
		f = nil
		return
	}
	//if e, ok := err.(*os.PathError); ok && e.Error == os.EEXIST {
	//    // file exist
	//    f = nil
	//    return
	//}
	return
}

// Get a param from http request
func getParam(req *http.Request, paramName string) (param string, err error) {
	values, ok := req.Form[paramName]
	if !ok {
		err = fmt.Errorf("Miss param: %v", paramName)
		return
	}
	param = strings.TrimSpace(values[0])
	if len(param) == 0 {
		err = fmt.Errorf("Miss param: %v", paramName)
		return
	}
	return param, nil
}

// Get list of params from http request
func parseParams(req *http.Request, paramNames []string) (params []string, err error) {
	params = make([]string, len(paramNames))
	for i, paramName := range paramNames {
		param, err := getParam(req, paramName)
		if err != nil {
			return params, err
		}
		params[i] = param
	}

	return params, nil
}

// Gen path by sparse name
func sparsePath(name string) string {
	return filepath.Join(name[:2], name[2:4], name)
}

// Get block file full path
func getBlockPath(blockInfo BlockInfo) (blockPath string, err error) {
	devicePath, ok := LOCAL_DEVICES.LocalDevicePath(blockInfo.Device)
	if !ok {
		err = fmt.Errorf("Not exist local device %s.", blockInfo.Device)
		return
	}
	sparsedPath := sparsePath(blockInfo.Hash) + "." + blockInfo.Type
	blockPath = filepath.Join(devicePath, BLOCKS_DIR, sparsedPath)
	return blockPath, nil
}

// For tests
// Get object hash from test file
func GetIndexhash(objectContent []byte) (objecthash string) {
	blockhashes := make([]string, 0)
	size := len(objectContent)
	var start int
	for start = 0; start <= size; start += BLOCK_SIZE {
		end := start + BLOCK_SIZE
		if end > size {
			end = size
		}
		block := objectContent[start:end]
		blockhash := Sha1Hex(block)
		blockhashes = append(blockhashes, blockhash)
		if end == size {
			break
		}
	}
	return Sha1Hex([]byte(strings.Join(blockhashes, "")))
}

// Split source string into sub array with given size.
func group(s string, n int) []string {
	num := len(s) / n
	subs := make([]string, num)
	for i := 0; i < num; i++ {
		subs[i] = s[i*n : (i+1)*n]
	}
	return subs
}

// When server starting up, terminate if get fatal error
func checkFatal(err error) {
	if err != nil {
		logger.Error("Fatal error: %s", err.Error())
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

// Get backtrace for error logging
func stack() string {
	buf := make([]byte, 2048)
	s := string(buf[:runtime.Stack(buf, false)])
	// ignore utils.go and log.go
	return strings.Join(strings.Split(s, "\n")[5:], "\n")
}

// Get IP from address
func IPFromAddr(addr string) string {
	return strings.Split(addr, ":")[0]
}

// Print int string
func Itoa(i interface{}) string {
	return fmt.Sprintf("%d", i)
}

// Divide
func DivideWithRemain(v int64, d int64) (r int32) {
	remain := int32(0)
	if v%d != 0 {
		remain = int32(1)
	}
	return int32(v/d) + remain
}

// Get header from local block file
func getHeaderFromBlockFile(blockInfo BlockInfo) (blockHeader BlockHeader, err error) {
	pdata, err := DEVICE_MANAGER.ReadFile(blockInfo, 0, HEADER_SIZE)
	if err != nil {
		return
	}
	headerBytes := *pdata
	if len(headerBytes) != HEADER_SIZE {
		err = fmt.Errorf("Not enough header %s", blockInfo)
		return
	}
	blockHeader, err = LoadBlockHeader(headerBytes)
	if err != nil {
		return
	}
	return blockHeader, nil
}

// Convert bool to string
func boolToString(b bool) (s string) {
	if b {
		return "1"
	}
	return "0"
}

// Convert string to bool
func boolFromSting(s string) (b bool) {
	if strings.TrimSpace(s) == "0" || strings.ToLower(strings.TrimSpace(s)) == "false" {
		return false
	}
	return true
}

// unrecoverable panic
func onFatalPanic() {
	if e := recover(); e != nil {
		logger.Critical("Fatal: %s", e)
		logger.Critical(stack())
		os.Exit(1)
	}
}

// recoverable panic
func onRecoverablePanic() {
	if e := recover(); e != nil {
		logger.Critical("Fatal: %s", e)
		logger.Critical(stack())
	}
}

// convert to uint64
func toUint64(v interface{}) (v1 uint64, err error) {
	switch v := v.(type) {
	case int:
		v1 = uint64(v)
	case uint:
		v1 = uint64(v)
	case int8:
		v1 = uint64(v)
	case uint8:
		v1 = uint64(v)
	case int16:
		v1 = uint64(v)
	case uint16:
		v1 = uint64(v)
	case int32:
		v1 = uint64(v)
	case uint32:
		v1 = uint64(v)
	case int64:
		v1 = uint64(v)
	case float32:
		v1 = uint64(v)
	case float64:
		v1 = uint64(v)
	case uint64:
		v1 = v
	}
	return v1, nil
}

func IsNumeric(s string) bool {
	for _, rune := range s {
		if !unicode.IsDigit(rune) {
			return false
		}
	}
	return true
}

func toInt(s string) (i int) {
	i, _ = strconv.Atoi(s)
	return
}

// Return a SHA1 hash hex for a specified string.
func Sha1Hex(q []byte) string {
	g := sha1.New()
	g.Write(q)
	return hex.EncodeToString(g.Sum(nil))
}

// Return a SHA1 hash bytes for a specified string.
func Sha1Bytes(q []byte) []byte {
	g := sha1.New()
	g.Write(q)
	return g.Sum(nil)
}

// Fetch http resource
func HttpGet(url string) (pbody *[]byte, err error) {
	resp, err := http.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	return &body, err
}

//// Pick an random element from a map
//func ChoiceMap(m map[string]interface{}) (string) {
//    keys := make([]string, 0)
//    for key := range m {
//        keys = append(keys, key)
//    }
//    return ChoiceString(keys)
//}

// Pick an random element from a slice
func ChoiceString(seq []string) (string, error) {
	if len(seq) == 0 || seq == nil {
		return "", fmt.Errorf("Empty slice")
	}
	return seq[rand.Int63n(int64(len(seq)))], nil
}

// Pick an random element from a slice
func ChoiceScoreSeg(seq []ScoreSeg) (scoreSeg ScoreSeg, err error) {
	if len(seq) == 0 || seq == nil {
		err = fmt.Errorf("Empty slice")
		return
	}
	return seq[rand.Int63n(int64(len(seq)))], nil
}

// Convert hash hex to bytes
func HashHexToBytes(h string, hexLength int) (hb []byte, err error) {
	if len(h) != hexLength {
		err = fmt.Errorf("Invalid hash hex length.")
		return
	}

	hb, err = hex.DecodeString(h)
	if err != nil {
		return
	}

	return
}

// Convert hash bytes to hex
func HashBytesToHex(hb []byte) (h string) {
	return hex.EncodeToString(hb)
}

// Convert disk total space
func getDiskspace(path string) (diskspace uint64) {
	buf := new(syscall.Statfs_t)
	syscall.Statfs(path, buf)
	//{61267 4096 7596115 5269220 4883360 1929536 1906540 {[423621017 1394364693]} 255 4096 0 [0 0 0 0]}
	return uint64(buf.Bsize) * buf.Blocks
}

//// Check if string/bytes starts with m
//func startswith(s interface{}, m interface{}) (b bool) {
//    var s1, m1 []byte

//	switch s := s.(type) {
//    case string:
//        s1 = []byte(s)
//    case []byte:
//        s1 = s
//    default:
//        return false
//    }

//	switch m := m.(type) {
//    case string:
//        m1 = []byte(m)
//    case []byte:
//        m1 = m
//    default:
//        return false
//	}

//    if len(s1) < len(m1) {
//        return false
//    }
//    if bytes.Equal(s1[0:len(m1)], m1) {
//        return true
//    }
//    return false
//}

//// Check if string/bytes ends with m
//func endswith(s interface{}, m interface{}) (b bool) {
//    var s1, m1 []byte

//	switch s := s.(type) {
//    case string:ntn returns, as an int, a non-ne
//        s1 = []byte(s)
//    case []byte:
//        s1 = s
//    default:
//        return false
//    }

//	switch m := m.(type) {
//    case string:
//        m1 = []byte(m)
//    case []byte:
//        m1 = m
//    default:
//        return false
//	}

//    if len(s1) < len(m1) {
//        return false
//    }
//    if bytes.Equal(s1[len(s1)-len(m1):len(s1)], m1) {
//        return true
//    }
//    return false
//}
