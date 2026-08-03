package vllm

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type backendSink struct{}

func (backendSink) Write(context.Context, domain.StreamEvent) error { return nil }

type exactRequestBinding struct {
	RequestID              string `json:"request_id"`
	ProviderRequestID      string `json:"provider_request_id"`
	ProviderManifestDigest string `json:"provider_manifest_digest"`
}

func testIdentity(t *testing.T) domain.WorkloadIdentity {
	t.Helper()
	id, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: "c", Namespace: "n", LogicalEngine: "e", PodUID: "pod-uid", EndpointEpoch: 7, RecoveryEpoch: 9})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func certificateFixture(t *testing.T, id domain.WorkloadIdentity, claims PeerIdentityClaims) (tls.Certificate, *x509.Certificate, *x509.CertPool) {
	t.Helper()
	now := time.Now()
	rootKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	rootTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "root"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, &rootKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	root, _ := x509.ParseCertificate(rootDER)
	leafKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	uri, _ := id.SPIFFEID("example.test")
	raw, _ := json.Marshal(claims)
	leafTemplate := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "proxy"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, URIs: []*url.URL{uri}, ExtraExtensions: []pkix.Extension{{Id: PeerClaimsExtensionOID(), Value: raw}}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, root, &leafKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(leafDER)
	pool := x509.NewCertPool()
	pool.AddCert(root)
	return tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: leafKey, Leaf: leaf}, leaf, pool
}

func TestVerifyPeerIdentityExactClaims(t *testing.T) {
	id := testIdentity(t)
	claims := PeerIdentityClaims{id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch(), "m", "sha256:" + strings.Repeat("a", 64), "sha256:" + strings.Repeat("b", 64)}
	_, leaf, roots := certificateFixture(t, id, claims)
	chains, err := leaf.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
	if err != nil {
		t.Fatal(err)
	}
	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: chains}
	if err = VerifyPeerIdentity(state, id); err != nil {
		t.Fatal(err)
	}
	wrong, _ := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: "c", Namespace: "n", LogicalEngine: "e", PodUID: "replacement", EndpointEpoch: 7, RecoveryEpoch: 9})
	if VerifyPeerIdentity(state, wrong) == nil {
		t.Fatal("same-name replacement identity accepted")
	}
}

func TestManifestVerifierRequiresValidSignatureAndPin(t *testing.T) {
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now().UTC()
	digestText := "sha256:" + strings.Repeat("a", 64)
	payload, err := json.Marshal(manifestPayload{ID: "m", SchemaVersion: 1, CapabilityVersion: 1, SignatureVersion: 1, ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), ImageDigest: digestText, ProxyDigest: digestText, Routes: []string{"/v1/chat/completions"}, Fields: []string{"model"}, Parser: "parser-v1", IdentityProfile: domain.IdentityExactWorkloadMTLS, APCIsolation: domain.APCDisabled})
	if err != nil {
		t.Fatal(err)
	}
	pin := sha256.Sum256(payload)
	verifier, err := NewManifestVerifier(map[string]ed25519.PublicKey{"k": public}, map[string]string{"m": hex.EncodeToString(pin[:])}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	signature := ed25519.Sign(private, payload)
	if _, err = verifier.Verify(SignedManifest{"k", payload, signature}); err != nil {
		t.Fatalf("valid signed pinned manifest rejected: %v", err)
	}
	tampered := append([]byte(nil), payload...)
	tampered[len(tampered)-1] ^= 1
	if _, err = verifier.Verify(SignedManifest{"k", tampered, signature}); err == nil {
		t.Fatal("tampered manifest accepted")
	}
}

func TestParseLoadMissingSeriesIsUnknown(t *testing.T) {
	_, err := parseLoad([]byte("other 0\n"), testIdentity(t), []string{"vllm:num_requests_running"}, time.Now())
	if err == nil {
		t.Fatal("missing metric became zero load")
	}
	observation, err := parseLoad([]byte("vllm:num_requests_running 0\n"), testIdentity(t), []string{"vllm:num_requests_running"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if value, known := observation.UsedSlots(); !known || value != 0 {
		t.Fatal("explicit zero not preserved")
	}
}

func TestBackendDoesNotFollowRedirect(t *testing.T) {
	id := testIdentity(t)
	image, proxy := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
	claims := PeerIdentityClaims{id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch(), "m", image, proxy}
	certificate, _, roots := certificateFixture(t, id, claims)
	calls := 0
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Redirect(w, r, "/again", http.StatusTemporaryRedirect)
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13}
	server.StartTLS()
	defer server.Close()
	manifest, err := domain.NewCapabilityManifest(domain.CapabilityManifestParams{ID: "m", SchemaVersion: 1, CapabilityVersion: 1, SignatureVersion: 1, SignatureVerified: true, VerifiedAt: time.Now(), ValidFrom: time.Now().Add(-time.Hour), ValidUntil: time.Now().Add(time.Hour), ImageDigest: image, ProxyDigest: proxy, Routes: []string{"/v1/chat/completions"}, Fields: []string{"model"}, Parser: "p", IdentityProfile: domain.IdentityExactWorkloadMTLS, APCIsolation: domain.APCDisabled})
	if err != nil {
		t.Fatal(err)
	}
	backend, err := NewBackend(BackendConfig{TLSConfig: &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{certificate}}, ResponseHeaderTimeout: time.Second, DialTimeout: time.Second, MaxEventBytes: 4096, MaxEvents: 8, MaxResponseBytes: 4096, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	target, _ := domain.NewDispatchTarget(server.URL, id)
	response, err := backend.do(context.Background(), http.MethodPost, target, manifest, "/v1/chat/completions", strings.NewReader("{}"), 2, "application/json", "application/json")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusTemporaryRedirect || calls != 1 {
		t.Fatalf("status=%d calls=%d", response.StatusCode, calls)
	}
}

func TestTerminationAcknowledgementBindsExactInferencePOSTAndManifest(t *testing.T) {
	id := testIdentity(t)
	image, proxy := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
	claims := PeerIdentityClaims{id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch(), "m", image, proxy}
	certificate, _, roots := certificateFixture(t, id, claims)
	now := time.Now()
	manifest, err := domain.NewCapabilityManifest(domain.CapabilityManifestParams{ID: "m", SchemaVersion: 1, CapabilityVersion: 1, SignatureVersion: 1, SignatureVerified: true, VerifiedAt: now, ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), ImageDigest: image, ProxyDigest: proxy, Routes: []string{"/v1/chat/completions", "/v1/prudentia/terminate"}, Fields: []string{"model", "prudentia_request_binding"}, Parser: "p", IdentityProfile: domain.IdentityExactWorkloadMTLS, APCIsolation: domain.APCDisabled, Termination: true})
	if err != nil {
		t.Fatal(err)
	}
	var original exactRequestBinding
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if r.URL.Path == "/v1/chat/completions" {
			var body struct {
				Binding exactRequestBinding `json:"prudentia_request_binding"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil {
				t.Error("invalid inference payload")
			}
			original = body.Binding
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"choices":[]}`))
			return
		}
		var termination exactRequestBinding
		if json.NewDecoder(r.Body).Decode(&termination) != nil {
			t.Error("invalid termination payload")
		}
		if termination != original {
			t.Error("termination did not reference exact inference POST")
		}
		_ = json.NewEncoder(w).Encode(struct {
			RequestID              string `json:"request_id"`
			ProviderRequestID      string `json:"provider_request_id"`
			PodUID                 string `json:"pod_uid"`
			ProviderManifestDigest string `json:"provider_manifest_digest"`
			EndpointEpoch          uint64 `json:"endpoint_epoch"`
			RecoveryEpoch          uint64 `json:"recovery_epoch"`
			Sequence               uint64 `json:"sequence"`
			Stopped                bool   `json:"stopped"`
		}{original.RequestID, original.ProviderRequestID, id.PodUID(), original.ProviderManifestDigest, id.EndpointEpoch(), id.RecoveryEpoch(), 1, true})
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13}
	server.StartTLS()
	defer server.Close()
	backend, err := NewBackend(BackendConfig{TLSConfig: &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{certificate}}, ResponseHeaderTimeout: time.Second, DialTimeout: time.Second, MaxEventBytes: 4096, MaxEvents: 8, MaxResponseBytes: 4096, Now: func() time.Time { return now }, ResolveEndpoint: func(domain.WorkloadIdentity) (string, error) { return server.URL, nil }})
	if err != nil {
		t.Fatal(err)
	}
	requestID, _ := domain.NewRequestID("request-1")
	principal, _ := domain.NewPrincipal("tenant", []string{"model"})
	inference, err := domain.NewInferenceRequest(domain.InferenceRequestParams{RequestID: requestID, Model: "model", Messages: []domain.MessageParams{{Role: "user", Content: "prompt"}}, MaxCompletionTokens: 8, Priority: domain.PriorityNormal, Features: domain.EmptyFeatureSet(), CachePolicy: domain.CachePolicyDisabled, ExecutionBudget: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	target, _ := domain.NewDispatchTarget(server.URL, id)
	call, err := domain.NewBackendCall(domain.BackendCallParams{Request: domain.NewAuthorizedRequest(principal, inference), Target: target, Manifest: manifest, ProviderRequestID: "provider-request-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = backend.Infer(context.Background(), call, backendSink{}); err != nil {
		t.Fatal(err)
	}
	if original.RequestID != "request-1" || original.ProviderRequestID != "provider-request-1" || original.ProviderManifestDigest != manifest.PayloadDigestString() {
		t.Fatal("inference POST omitted exact termination binding")
	}
	ref, err := domain.NewProviderRequestRef(domain.ProviderRequestRefParams{Tenant: "tenant", RequestID: "request-1", ProviderRequestID: "provider-request-1", Target: id, Manifest: manifest, ExpiresAt: now.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = backend.Terminate(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
}

func TestStockManifestCannotClaimTermination(t *testing.T) {
	now := time.Now()
	digest := "sha256:" + strings.Repeat("a", 64)
	_, err := domain.NewCapabilityManifest(domain.CapabilityManifestParams{ID: "stock", SchemaVersion: 1, CapabilityVersion: 1, SignatureVersion: 1, SignatureVerified: true, VerifiedAt: now, ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), ImageDigest: digest, ProxyDigest: digest, Routes: []string{"/v1/chat/completions"}, Fields: []string{"model"}, Parser: "stock", IdentityProfile: domain.IdentityExactWorkloadMTLS, APCIsolation: domain.APCDisabled, Termination: true})
	if err == nil {
		t.Fatal("stock path enabled termination without exact request binding route and field")
	}
}
