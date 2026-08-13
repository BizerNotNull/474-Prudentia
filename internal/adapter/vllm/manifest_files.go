package vllm

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type ProviderSecurityFiles struct {
	CertFile              string
	KeyFile               string
	CAFile                string
	ManifestPayloadFile   string
	ManifestSignatureFile string
	ManifestKeyID         string
	ManifestID            string
	ManifestPublicKey     string
	ManifestPin           string
}

type ProviderSecurity struct {
	TLSConfig *tls.Config
	Manifest  domain.CapabilityManifest
}

// LoadProviderSecurity loads the shared exact-provider trust material used by
// request and observation clients.
func LoadProviderSecurity(files ProviderSecurityFiles, now func() time.Time) (ProviderSecurity, error) {
	certificate, err := tls.LoadX509KeyPair(files.CertFile, files.KeyFile)
	if err != nil {
		return ProviderSecurity{}, fmt.Errorf("load provider TLS identity: %w", err)
	}
	data, err := os.ReadFile(files.CAFile)
	if err != nil {
		return ProviderSecurity{}, fmt.Errorf("load provider CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return ProviderSecurity{}, errors.New("provider CA file contains no certificate")
	}
	manifest, err := LoadVerifiedManifest(
		files.ManifestPayloadFile, files.ManifestSignatureFile, files.ManifestKeyID, files.ManifestID,
		files.ManifestPublicKey, files.ManifestPin, now,
	)
	if err != nil {
		return ProviderSecurity{}, fmt.Errorf("load pinned provider manifest: %w", err)
	}
	return ProviderSecurity{
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{certificate},
			RootCAs:      roots,
			MinVersion:   tls.VersionTLS13,
		},
		Manifest: manifest,
	}, nil
}

// LoadVerifiedManifest reads bounded deployment material and returns only the
// immutable verified form. Raw signatures and payloads are not retained.
func LoadVerifiedManifest(payloadFile, signatureFile, keyID, manifestID, publicKeyBase64, payloadPin string, now func() time.Time) (domain.CapabilityManifest, error) {
	if payloadFile == "" || signatureFile == "" || keyID == "" || manifestID == "" || publicKeyBase64 == "" || payloadPin == "" || now == nil {
		return domain.CapabilityManifest{}, errors.New("complete capability manifest configuration is required")
	}
	payload, err := os.ReadFile(payloadFile)
	if err != nil || len(payload) == 0 || len(payload) > 64<<10 {
		return domain.CapabilityManifest{}, errors.New("read bounded capability manifest")
	}
	signatureEncoded, err := os.ReadFile(signatureFile)
	if err != nil || len(signatureEncoded) == 0 || len(signatureEncoded) > 1024 {
		return domain.CapabilityManifest{}, errors.New("read bounded capability manifest signature")
	}
	signature, err := base64.StdEncoding.DecodeString(string(signatureEncoded))
	if err != nil {
		return domain.CapabilityManifest{}, errors.New("decode capability manifest signature")
	}
	publicKey, err := base64.StdEncoding.DecodeString(publicKeyBase64)
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return domain.CapabilityManifest{}, errors.New("decode capability manifest public key")
	}
	verifier, err := NewManifestVerifier(map[string]ed25519.PublicKey{keyID: publicKey}, map[string]string{manifestID: payloadPin}, now)
	if err != nil {
		return domain.CapabilityManifest{}, err
	}
	return verifier.Verify(SignedManifest{KeyID: keyID, Payload: payload, Signature: signature})
}
