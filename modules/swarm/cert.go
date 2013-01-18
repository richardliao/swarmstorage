package swarm

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
)

type DomainCert struct {
	DomainId  string
	DomainKey rsa.PrivateKey
}

type NodeCert struct {
	DomainId string
	NodeId   string
}

// Create rsa private key
func NewRsaKey() (priv *rsa.PrivateKey, err error) {
	priv, err = rsa.GenerateKey(rand.Reader, 512)
	if err != nil {
		return
	}
	return
}

// Encode to base64 encoded gob
func EncodeB64Gob(obj interface{}) (encoded []byte, err error) {
	buff := new(bytes.Buffer)

	b64Encoder := base64.NewEncoder(base64.StdEncoding, buff)
	gobEncoder := gob.NewEncoder(b64Encoder)

	err = gobEncoder.Encode(obj)
	if err != nil {
		return
	}
	// Close here to flush to buffer
	b64Encoder.Close()

	encoded = buff.Bytes()
	return
}

// Decode base64 encoded gob
func DecodeB64Gob(encoded []byte, t string) (obj interface{}, err error) {
	switch t {
	case "DomainCert":
		obj = new(DomainCert)
	case "DomainPubKey":
		obj = new(rsa.PublicKey)
	case "NodeSig":
		obj = new([]byte)
	default:
		err = fmt.Errorf("Invalid type: " + t)
		return
	}
	err = gob.NewDecoder(base64.NewDecoder(base64.StdEncoding, bytes.NewReader(encoded))).Decode(obj)
	return
}

// Load domain pub key
func loadDomainPubKey(encoded []byte) (domainPubKey *rsa.PublicKey, err error) {
	obj, err := DecodeB64Gob(encoded, "DomainPubKey")
	if err != nil {
		return
	}

	domainPubKey, ok := obj.(*rsa.PublicKey)
	if !ok {
		err = fmt.Errorf("Failed to convert obj to DomainPubKey")
		return nil, err
	}
	return domainPubKey, nil
}

// Load node sig
func loadNodeSig(nodeSigBase64 string) (nodeSig []byte, err error) {
	nodeSig, err = base64.StdEncoding.DecodeString(nodeSigBase64)
	return
}

// Sig node cert
func DoNodeSig(nodeCert *NodeCert, domainKey *rsa.PrivateKey) (nodeSigBase64 string, err error) {
	// convert to json
	encoded, err := json.Marshal(nodeCert)
	if err != nil {
		return
	}

	// sign
	nodeSig, err := rsa.SignPKCS1v15(rand.Reader, domainKey, crypto.SHA1, Sha1Bytes(encoded))
	if err != nil {
		return
	}

	// base64
	nodeSigBase64 = base64.StdEncoding.EncodeToString(nodeSig)
	return
}

// Verify gossip GossipProbeResponse.
// A very basic certification, only verify node sig for domain id and node id.
func verifyGossipProbeResponse(resp *GossipProbeResponse, remoteIp string) (err error) {
	// ignore any error
	defer func() {
		if e := recover(); e != nil {
			logger.Critical("Fatal: %s", e)
			logger.Critical(stack())
			err = fmt.Errorf("Fatal error verify gossip response, error: %s", e)
			return
		}
	}()

	// verify NodeSig
	encoded, err := json.Marshal(resp.NodeCert)
	if err != nil {
		return
	}
	if err = rsa.VerifyPKCS1v15(DOMAIN_PUB_KEY, crypto.SHA1, Sha1Bytes(encoded), resp.NodeSig); err != nil {
		return
	}
	// verify DomainId
	if resp.NodeCert.DomainId != DOMAIN_ID {
		err = fmt.Errorf("Mismatch domain id: %s", resp.NodeCert.DomainId)
		return
	}
	// verify NodeId
	if resp.NodeCert.NodeId != remoteIp {
		err = fmt.Errorf("Mismatch node id: %s for node %s", resp.NodeCert.NodeId, remoteIp)
		return
	}
	// verify domain config sig
	if resp.DomainConfigSig != DOMAIN_CONFIG_SIG {
		err = fmt.Errorf("Mismatch domain config sig: %s for node %s", resp.DomainConfigSig, remoteIp)
		return
	}

	return
}
