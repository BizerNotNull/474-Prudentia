package contract_test

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"github.com/BizerNotNull/474-Prudentia/internal/adapter/vllm"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type guardedBody struct {
	reads int
	body  *bytes.Reader
}

func (b *guardedBody) Read(p []byte) (int, error) { b.reads++; return b.body.Read(p) }

func peerState(t *testing.T, identity domain.WorkloadIdentity) tls.ConnectionState {
	t.Helper()
	uri, err := identity.SPIFFEID("test.local")
	if err != nil {
		t.Fatal(err)
	}
	claims, _ := json.Marshal(vllm.PeerIdentityClaims{PodUID: identity.PodUID(), EndpointEpoch: identity.EndpointEpoch(), RecoveryEpoch: identity.RecoveryEpoch(), ManifestID: "manifest-v1", ImageDigest: "sha256:" + strings.Repeat("a", 64), ProxyDigest: "sha256:" + strings.Repeat("b", 64)})
	leaf := &x509.Certificate{Raw: []byte("leaf"), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, URIs: []*url.URL{uri}, Extensions: []pkix.Extension{{Id: vllm.PeerClaimsExtensionOID(), Value: claims}}}
	return tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf}}}
}

func exposeBodyOnlyAfterIdentity(state tls.ConnectionState, expected domain.WorkloadIdentity, body *guardedBody) error {
	if err := vllm.VerifyPeerIdentity(state, expected); err != nil {
		return err
	}
	buffer := make([]byte, 1)
	_, err := body.Read(buffer)
	return err
}

func TestPodReplacementFailsExactIdentityBeforeBodyRead(t *testing.T) {
	oldIdentity, _ := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: "cluster", Namespace: "models", LogicalEngine: "engine", PodUID: "old-pod-uid", EndpointEpoch: 4, RecoveryEpoch: 1})
	replacement, _ := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: "cluster", Namespace: "models", LogicalEngine: "engine", PodUID: "replacement-pod-uid", EndpointEpoch: 5, RecoveryEpoch: 1})
	state := peerState(t, replacement)
	body := &guardedBody{body: bytes.NewReader([]byte("raw prompt bytes"))}
	if err := exposeBodyOnlyAfterIdentity(state, oldIdentity, body); err == nil {
		t.Fatal("same endpoint replacement satisfied stale workload expectation")
	}
	if body.reads != 0 {
		t.Fatalf("body was read %d time(s) before identity match", body.reads)
	}
	if err := exposeBodyOnlyAfterIdentity(state, replacement, body); err != nil {
		t.Fatal(err)
	}
	if body.reads != 1 {
		t.Fatalf("body reads = %d, want 1 after exact match", body.reads)
	}
}
