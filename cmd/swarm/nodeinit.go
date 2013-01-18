package main

import (
	"fmt"
	"github.com/richardliao/swarm/modules/swarm"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

var cmdNodeinit = &Command{
	UsageLine: "nodeinit options",
	Short:     "init swarm node",
	Long: `
Command nodeinit create new swarm node environment specified in nodebase flag.
	`,
}

var (
	nodeinitNodebase       = cmdNodeinit.Flag.String("nodebase", "/data/swarm", "Path to the new swarm node environment, must exist")
	nodeinitDomaincert     = cmdNodeinit.Flag.String("domaincert", "", "Path to the domain cert file")
	nodeinitNodeaddress    = cmdNodeinit.Flag.String("nodeaddress", "", "Node address")
	nodeinitOSSaddress     = cmdNodeinit.Flag.String("ossaddress", "", "Object server address, can be same with nodeaddress")
)

func init() {
	cmdNodeinit.Run = runNodeinit
}

func runNodeinit(cmd *Command, args []string) {
	// verify flags
	if len(*nodeinitNodebase) == 0 || len(*nodeinitDomaincert) == 0 || len(*nodeinitNodeaddress) == 0 || len(*nodeinitOSSaddress) == 0 {
		fmt.Fprintf(os.Stderr, "Invalid params.\n")
		os.Exit(1)
	}

	// parse cert
	domainCert, err := loadDomainCert()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Load key failed: %s\n", err)
		os.Exit(1)
	}

	// domain pub key
	domainPubKeyEncoded, err := swarm.EncodeB64Gob(domainCert.DomainKey.PublicKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Gob encode failed: %s\n", err)
		os.Exit(1)
	}

	// sig
	nodeCert := swarm.NodeCert{
		DomainId: domainCert.DomainId,
		NodeId:   *nodeinitNodeaddress,
	}

	nodeSigBase64, err := swarm.DoNodeSig(&nodeCert, &domainCert.DomainKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Sig node cert failed: %s\n", err)
		os.Exit(1)
	}

	// prepare dirs
	mkdir(swarm.CONF_DIR)
	mkdir(swarm.DEVICES_DIR)
	mkdir(swarm.LOGS_DIR)

	// prepare files
	targetPath := path.Join(*nodeinitNodebase, swarm.CONF_DIR, swarm.NODE_CONF_FILE)
	f, err := swarm.CreateFile(targetPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed create file %s, error: %s.\n", targetPath, err)
		os.Exit(1)
	}
	defer f.Close()

	// write config file
	defaultConfig := []string{}
	defaultConfig = append(defaultConfig, "[default]")
	defaultConfig = append(defaultConfig, fmt.Sprintf("LOG_FILE=%s", "logs/swarm.log"))
	defaultConfig = append(defaultConfig, fmt.Sprintf("NODE_ADDRESS=%s", *nodeinitNodeaddress))
	defaultConfig = append(defaultConfig, fmt.Sprintf("OSS_ADDRESS=%s", *nodeinitOSSaddress))
	defaultConfig = append(defaultConfig, fmt.Sprintf("DOMAIN_ID=%s", domainCert.DomainId))
	defaultConfig = append(defaultConfig, fmt.Sprintf("DOMAIN_PUB_KEY=%s", domainPubKeyEncoded))
	defaultConfig = append(defaultConfig, fmt.Sprintf("NODE_SIG=%s", nodeSigBase64))
	defaultConfig = append(defaultConfig, "")

	f.Write([]byte(strings.Join(defaultConfig, "\n")))

	fmt.Printf("Node %s initiated.\n", *nodeinitNodebase)
}

// Make dir
func mkdir(subdir string) {
	targetPath := path.Join(*nodeinitNodebase, subdir)
	err := os.MkdirAll(targetPath, 0755)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed make dir %s, error: %s.\n", targetPath, err)
		os.Exit(1)
	}
}

// Load domain cert
func loadDomainCert() (domainCert *swarm.DomainCert, err error) {
	encoded, err := ioutil.ReadFile(*nodeinitDomaincert)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed open file %s, error: %s\n", *nodeinitDomaincert, err)
		os.Exit(1)
	}

	obj, err := swarm.DecodeB64Gob(encoded, "DomainCert")
	if err != nil {
		fmt.Fprintf(os.Stderr, "DecodeB64Gob failed: %s\n", err)
		os.Exit(1)
	}

	switch c := obj.(type) {
	case *swarm.DomainCert:
		return c, nil
	default:
		err = fmt.Errorf("Failed to convert obj to DomainCert")
		return nil, err
	}
	return
}
