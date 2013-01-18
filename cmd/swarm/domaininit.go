package main

import (
	"fmt"
	"github.com/richardliao/swarm/modules/swarm"
	"os"
)

var cmdDomaininit = &Command{
	UsageLine: "domaininit options",
	Short:     "init swarm autonomous domain",
	Long: `
Command domaininit create new swarm autonomous domain certification.
	`,
}

var (
	domaininitDomainId = cmdDomaininit.Flag.String("domainid", "", "Domain ID, usually an UUID string")
)

func init() {
	cmdDomaininit.Run = runDomaininit
}

func runDomaininit(cmd *Command, args []string) {
	// verify flags
	if len(*domaininitDomainId) == 0 {
		fmt.Fprintf(os.Stderr, "Invalid params.\n")
		os.Exit(1)
	}

	domainKey, err := swarm.NewRsaKey()
	if err != nil {
		fmt.Fprintf(os.Stderr, "GenerateKey failed: %s\n", err)
		os.Exit(1)
	}

	// create domainCert
	domainCert := swarm.DomainCert{
		DomainId:  *domaininitDomainId,
		DomainKey: *domainKey,
	}

	// encode
	encoded, err := swarm.EncodeB64Gob(domainCert)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Gob encode failed: %s\n", err)
		os.Exit(1)
	}

	// write to file
	filePath := "/tmp/domain.cert"
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed open file %s, error: %s\n", filePath, err)
		os.Exit(1)
	}
	defer f.Close()
	f.Write(encoded)

	fmt.Println("Domain cert /tmp/domain.cert created, please keep it safely.")
}
