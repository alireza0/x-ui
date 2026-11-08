package network

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"
)

// testCertificate mints a self-signed certificate for the panel's domain, so a
// full handshake can run in memory.
func testCertificate(t *testing.T, domain string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: domain},
		DNSNames:     []string{domain},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// handshake runs one client hello against a server using config, and reports
// whether it succeeded plus how many certificates the client received.
func handshake(t *testing.T, config *tls.Config, serverName string) (error, int) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	// A stalled handshake should fail the test rather than hang the suite.
	deadline := time.Now().Add(10 * time.Second)
	clientConn.SetDeadline(deadline)
	serverConn.SetDeadline(deadline)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = tls.Server(serverConn, config).Handshake()
		// Close the pipe rather than the tls.Conn: closing the latter sends a
		// close_notify under a five second write deadline, which a synchronous
		// pipe with nobody reading would burn in full on every case.
		serverConn.Close()
	}()

	client := tls.Client(clientConn, &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: true, // the certificate is self-signed on purpose
	})
	err := client.Handshake()
	certificates := len(client.ConnectionState().PeerCertificates)
	clientConn.Close()
	wg.Wait()

	return err, certificates
}

func TestTLSConfigAcceptsTheConfiguredDomain(t *testing.T) {
	config := NewTLSConfig(testCertificate(t, "panel.example.com"), "panel.example.com")

	err, certificates := handshake(t, config, "panel.example.com")
	if err != nil {
		t.Fatalf("handshake for the configured domain failed: %v", err)
	}
	if certificates == 0 {
		t.Fatal("the client received no certificate")
	}
}

// The point of the check: a scanner that connects with the wrong name, or with
// none at all, must not be able to read the certificate and learn what domain
// this host serves.
func TestTLSConfigRejectsUnknownServerName(t *testing.T) {
	config := NewTLSConfig(testCertificate(t, "panel.example.com"), "panel.example.com")

	tests := []struct {
		name       string
		serverName string
	}{
		{"a different domain", "scanner.example.net"},
		// Go's client sends no SNI at all for an IP literal, so this is exactly
		// what a scanner hitting the panel by address looks like.
		{"no SNI, as when connecting by IP", ""},
		{"a subdomain of the real one", "a.panel.example.com"},
		{"a domain the real one is a suffix of", "notpanel.example.com"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err, certificates := handshake(t, config, test.serverName)
			if err == nil {
				t.Fatal("the handshake succeeded, want it rejected")
			}
			if certificates != 0 {
				t.Fatalf("the client received %d certificate(s); the point is to reveal none", certificates)
			}
		})
	}
}

// Names that a real client may present in a different but equivalent form must
// still be served, or a legitimate visitor is locked out.
func TestTLSConfigAcceptsEquivalentNames(t *testing.T) {
	config := NewTLSConfig(testCertificate(t, "panel.example.com"), "Panel.Example.COM")

	tests := []struct {
		name       string
		serverName string
	}{
		// DNS names are case-insensitive.
		{"different case", "panel.example.com"},
		{"same case as configured", "Panel.Example.COM"},
		// RFC 6066 forbids a trailing dot in SNI, and Go's client strips it
		// before sending, so an absolute FQDN arrives already normalised.
		{"absolute FQDN", "panel.example.com."},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err, certificates := handshake(t, config, test.serverName)
			if err != nil {
				t.Fatalf("handshake failed: %v", err)
			}
			if certificates == 0 {
				t.Fatal("the client received no certificate")
			}
		})
	}
}

// With no domain configured the panel is reachable by IP, which is the default
// and must keep working.
func TestTLSConfigWithoutDomainServesEveryone(t *testing.T) {
	config := NewTLSConfig(testCertificate(t, "panel.example.com"), "")

	for _, serverName := range []string{"", "anything.example.net", "panel.example.com"} {
		err, certificates := handshake(t, config, serverName)
		if err != nil {
			t.Errorf("handshake with SNI %q failed: %v", serverName, err)
		}
		if certificates == 0 {
			t.Errorf("no certificate served for SNI %q", serverName)
		}
	}
	if config.GetCertificate != nil {
		t.Error("no domain was configured, so no SNI callback should be installed")
	}
}

// Clients send no SNI when they connect by IP, so with the domain set to the
// server's address every handshake was refused.
func TestTLSConfigServesAnIPDomain(t *testing.T) {
	for _, domain := range []string{"192.0.2.1", "2001:db8::1"} {
		config := NewTLSConfig(testCertificate(t, "panel.example.com"), domain)

		if err, certificates := handshake(t, config, ""); err != nil || certificates == 0 {
			t.Errorf("domain %q: handshake without SNI failed: %v", domain, err)
		}
		if err, _ := handshake(t, config, "scanner.example.net"); err == nil {
			t.Errorf("domain %q: handshake with a foreign SNI succeeded, want it rejected", domain)
		}
	}
}
