package swarm

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Get ip addresses in the same subnet of bind address
// Return start ip and subnet size 
func getNeighbors() (startIpNum uint32, maskNum uint32) {
	// [127.0.0.1/8 192.168.200.1/29 ::1/128 fe80::213:72ff:fe86:305a/64 2001:0:53aa:64c:18d1:df7e:8eb5:d2d8/32 fe80::ffff:ffff:ffff/64]
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		logger.Error("Failed get neighbors, error ", err.Error())
		return
	}

	for _, addr := range addrs {
		parts := strings.SplitN(addr.String(), "/", 2)
		if len(parts) != 2 {
			continue
		}
		nodeIp, mask := parts[0], parts[1]
		if nodeIp == NODE_ADDRESS {
			//logger.Debug("Local ip: ", nodeIp, "/", mask)
			startIpNum, maskNum, err = ipSubnet(nodeIp, mask)
			if err == nil {
				break
			}
		}
	}
	return
}

// Convert dotted ip address to uint32
func dottedToNum(nodeIp string) (ipNum uint32) {
	parts := strings.Split(nodeIp, ".")
	if len(parts) != 4 {
		return 0
	}

	for i, part := range parts {
		partUint, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return 0
		}
		ipNum += (uint32(partUint) << uint8(8*(3-i)))
	}
	return ipNum
}

// Get subnet of specified ip
// Return start ip and subnet size 
func ipSubnet(nodeIp string, mask string) (startIpNum uint32, maskNum uint32, err error) {
	ipNum := dottedToNum(nodeIp)

	maskInt, err := strconv.ParseUint(mask, 10, 8)
	if err != nil {
		return
	}

	maskNum = uint32(2<<uint8(32-maskInt-1)) - 1
	startIpNum = ipNum &^ maskNum

	return startIpNum, maskNum, nil
}

// Convert int to dotted ip
func numToDotted(ipNum uint32) (nodeIp string) {
	nodeIp = fmt.Sprintf("%d.%d.%d.%d",
		(ipNum&0xff000000)>>24,
		(ipNum&0x00ff0000)>>16,
		(ipNum&0x0000ff00)>>8,
		(ipNum & 0x000000ff))
	return nodeIp
}
