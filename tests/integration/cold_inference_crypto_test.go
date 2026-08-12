//go:build integration

package integration_test

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/adapter/vllm"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

func newColdPaths(t *testing.T, providerDigest string) (coldPaths, manifestMaterial) {
	t.Helper()
	root := repositoryRoot(t)
	directory := t.TempDir()
	gateway, scheduler, proxy := buildColdBinaries(t, root, directory)
	paths := coldPaths{
		root:              root,
		caFile:            filepath.Join(directory, "ca.pem"),
		gatewayClientCert: filepath.Join(directory, "gateway-client.pem"),
		gatewayClientKey:  filepath.Join(directory, "gateway-client-key.pem"),
		gatewayPublicCert: filepath.Join(directory, "gateway-public.pem"),
		gatewayPublicKey:  filepath.Join(directory, "gateway-public-key.pem"),
		schedulerCert:     filepath.Join(directory, "scheduler.pem"),
		schedulerKey:      filepath.Join(directory, "scheduler-key.pem"),
		proxyCert:         filepath.Join(directory, "proxy.pem"),
		proxyKey:          filepath.Join(directory, "proxy-key.pem"),
		manifestPayload:   filepath.Join(directory, "manifest.json"),
		manifestSignature: filepath.Join(directory, "manifest.sig"),
		gatewayBinary:     gateway,
		schedulerBinary:   scheduler,
		proxyBinary:       proxy,
	}
	proxyDigest := sha256File(t, proxy)
	ca, caKey := writeTestCA(t, paths.caFile)
	writeLeaf(t, ca, caKey, paths.schedulerCert, paths.schedulerKey, leafSpec{
		commonName: "scheduler.test", dnsNames: []string{"scheduler.test"}, serverAuth: true,
	})
	gatewayURI := mustParseURI(t, coldGatewaySPIFFEID)
	writeLeaf(t, ca, caKey, paths.gatewayClientCert, paths.gatewayClientKey, leafSpec{
		commonName: "cold-gateway", uris: []*url.URL{gatewayURI}, clientAuth: true,
	})
	writeLeaf(t, ca, caKey, paths.gatewayPublicCert, paths.gatewayPublicKey, leafSpec{
		commonName: "localhost", dnsNames: []string{"localhost"}, ipAddresses: []net.IP{net.ParseIP("127.0.0.1")}, serverAuth: true,
	})
	identity := coldIdentity(t)
	proxyURI, err := identity.SPIFFEID(coldProviderTrustDomain)
	if err != nil {
		t.Fatalf("create proxy SPIFFE ID: %v", err)
	}
	claims, err := json.Marshal(vllm.PeerIdentityClaims{
		PodUID: coldPodUID, EndpointEpoch: coldEndpointEpoch, RecoveryEpoch: coldRecoveryEpoch,
		ManifestID: coldManifestID, ImageDigest: providerDigest, ProxyDigest: proxyDigest,
	})
	if err != nil {
		t.Fatalf("marshal proxy claims: %v", err)
	}
	writeLeaf(t, ca, caKey, paths.proxyCert, paths.proxyKey, leafSpec{
		commonName: "cold-identity-proxy", ipAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		uris: []*url.URL{proxyURI}, serverAuth: true,
		extraExtensions: []pkix.Extension{{Id: vllm.PeerClaimsExtensionOID(), Critical: false, Value: claims}},
	})
	manifest := writeManifest(t, paths, providerDigest, proxyDigest)
	return paths, manifest
}

type leafSpec struct {
	commonName      string
	dnsNames        []string
	ipAddresses     []net.IP
	uris            []*url.URL
	serverAuth      bool
	clientAuth      bool
	extraExtensions []pkix.Extension
}

func writeTestCA(t *testing.T, path string) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Prudentia cold-path test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	certificate, err := x509.ParseCertificate(raw)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	writePEM(t, path, "CERTIFICATE", raw)
	return certificate, key
}

func writeLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, certPath, keyPath string, spec leafSpec) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate %s key: %v", spec.commonName, err)
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		t.Fatalf("generate %s serial: %v", spec.commonName, err)
	}
	usages := make([]x509.ExtKeyUsage, 0, 2)
	if spec.serverAuth {
		usages = append(usages, x509.ExtKeyUsageServerAuth)
	}
	if spec.clientAuth {
		usages = append(usages, x509.ExtKeyUsageClientAuth)
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: spec.commonName},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(12 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usages,
		DNSNames: append([]string(nil), spec.dnsNames...), IPAddresses: append([]net.IP(nil), spec.ipAddresses...),
		URIs: append([]*url.URL(nil), spec.uris...), ExtraExtensions: append([]pkix.Extension(nil), spec.extraExtensions...),
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create %s certificate: %v", spec.commonName, err)
	}
	privateKey, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal %s key: %v", spec.commonName, err)
	}
	writePEM(t, certPath, "CERTIFICATE", raw)
	writePEM(t, keyPath, "PRIVATE KEY", privateKey)
}

func writePEM(t *testing.T, path, kind string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: data}), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func mustParseURI(t *testing.T, value string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(value)
	if err != nil {
		t.Fatalf("parse URI %q: %v", value, err)
	}
	return parsed
}

func writeManifest(t *testing.T, paths coldPaths, providerDigest, proxyDigest string) manifestMaterial {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate manifest key: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"id": coldManifestID, "schema_version": coldVersions.manifestSchema,
		"capability_version": coldVersions.manifestCapability, "signature_version": coldVersions.manifestSignature,
		"valid_from": time.Now().Add(-time.Hour).UTC(), "valid_until": time.Now().Add(12 * time.Hour).UTC(),
		"image_digest": providerDigest, "proxy_digest": proxyDigest,
		"routes": []string{"/health", "/v1/chat/completions"},
		"fields": []string{"max_completion_tokens", "messages", "model", "prudentia_request_binding", "stream"},
		"parser": "openai-chat-sse-v1", "identity_profile": domain.IdentityExactWorkloadMTLS, "apc_isolation": domain.APCDisabled,
	})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	signature := ed25519.Sign(privateKey, payload)
	if err := os.WriteFile(paths.manifestPayload, payload, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := os.WriteFile(paths.manifestSignature, []byte(base64.StdEncoding.EncodeToString(signature)), 0o600); err != nil {
		t.Fatalf("write manifest signature: %v", err)
	}
	pin := sha256.Sum256(payload)
	return manifestMaterial{
		publicKeyBase64: base64.StdEncoding.EncodeToString(publicKey), pin: hex.EncodeToString(pin[:]),
		providerDigest: providerDigest, proxyDigest: proxyDigest,
	}
}
