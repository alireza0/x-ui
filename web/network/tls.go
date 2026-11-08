package network

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"strings"
	"time"

	"github.com/alireza0/x-ui/util/common"
	utls "github.com/refraction-networking/utls"
)

func NewTLSConfig(cert tls.Certificate, domain string) *tls.Config {
	if domain == "" {
		return &tls.Config{Certificates: []tls.Certificate{cert}}
	}
	crt := cert
	serverName := domain
	if net.ParseIP(domain) != nil {
		// Clients send no SNI for an IP address (RFC 6066), so every
		// handshake was refused; the Host check still pins the address.
		serverName = ""
	}
	return &tls.Config{
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			if !strings.EqualFold(hello.ServerName, serverName) {
				return nil, common.NewErrorf("tls: unrecognized server name %q", hello.ServerName)
			}
			return &crt, nil
		},
	}
}

func GetCertHash(certFile string, certContent string) ([]string, error) {
	var certBytes []byte
	if path := strings.TrimSpace(certFile); path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		certBytes = b
	} else if strings.TrimSpace(certContent) != "" {
		certBytes = []byte(certContent)
	} else {
		return nil, common.NewError("no certificate provided")
	}

	var certs []*x509.Certificate
	if bytes.Contains(certBytes, []byte("BEGIN")) {
		rest := certBytes
		for {
			block, remain := pem.Decode(rest)
			if block == nil {
				break
			}
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, common.NewError("unable to decode certificate: ", err)
			}
			certs = append(certs, cert)
			rest = remain
		}
	} else {
		parsed, err := x509.ParseCertificates(certBytes)
		if err != nil {
			return nil, common.NewError("unable to parse certificates: ", err)
		}
		certs = parsed
	}

	if len(certs) == 0 {
		return nil, common.NewError("no certificates found")
	}

	hashes := make([]string, 0, len(certs))
	for _, cert := range certs {
		sum := sha256.Sum256(cert.Raw)
		hashes = append(hashes, hex.EncodeToString(sum[:]))
	}
	return hashes, nil
}

func GenerateSelfSignedCert(serverName string) (any, error) {
	var names []string
	for _, name := range strings.Split(serverName, ",") {
		if name = strings.TrimSpace(name); name != "" {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		names = []string{"localhost", "127.0.0.1", "::1"}
	}

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, common.NewError("unable to generate private key: ", err)
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, common.NewError("unable to generate serial number: ", err)
	}

	now := time.Now()
	template := x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               pkix.Name{CommonName: names[0]},
		NotBefore:             now.Add(-24 * time.Hour),
		NotAfter:              now.AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	for _, name := range names {
		if ip := net.ParseIP(name); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, name)
		}
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, common.NewError("unable to create certificate: ", err)
	}
	keyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, common.NewError("unable to encode private key: ", err)
	}

	certObj := map[string]string{
		"cert": string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBytes})),
		"key":  string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})),
	}

	return certObj, nil
}

func GetTlsPing(domain string, port string) (any, error) {
	if domain == "" {
		return "", common.NewError("domain is empty")
	}
	if port == "" {
		port = "443"
	}

	d := net.Dialer{Timeout: 10 * time.Second}
	tcpConn, err := d.Dial("tcp", domain+":"+port)
	if err != nil {
		return "", common.NewErrorf("Failed to dial tcp: %s", err)
	}
	tlsConn := utls.UClient(tcpConn, &utls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2", "http/1.1"},
	}, utls.HelloChrome_Auto)
	err = tlsConn.Handshake()
	if err != nil {
		return "", common.NewErrorf("Failed to handshake: %s", err)
	}
	var leaf *x509.Certificate
	for _, cert := range tlsConn.ConnectionState().PeerCertificates {
		if len(cert.DNSNames) != 0 {
			leaf = cert
			break
		}
	}
	leafHash := sha256.Sum256(leaf.Raw)
	leafObj := map[string]string{
		"leafHash": hex.EncodeToString(leafHash[:]),
	}

	return leafObj, nil

}
