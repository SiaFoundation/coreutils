package certs

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"

	"lukechampine.com/frand"
)

// An EphemeralCertManager is an in-memory minimal rhp4.CertManager for testing.
// Calls to GetCertificate will return a new self-signed certificate each time.
type EphemeralCertManager struct{}

// GetCertificate returns a new self-signed certificate each time it is called.
func (ec *EphemeralCertManager) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	key, err := rsa.GenerateKey(frand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(frand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cert: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to create tls cert: %w", err)
	}
	return &cert, nil
}
