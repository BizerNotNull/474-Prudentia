package vllm

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/asn1"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

var peerClaimsOID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 57264, 1, 1}

// PeerIdentityClaims is the signed certificate extension issued to the exact
// per-Pod proxy. It is exported so identity issuers can produce the claim.
type PeerIdentityClaims struct {
	PodUID        string `json:"pod_uid"`
	EndpointEpoch uint64 `json:"endpoint_epoch"`
	RecoveryEpoch uint64 `json:"recovery_epoch"`
	ManifestID    string `json:"manifest_id"`
	ImageDigest   string `json:"image_digest"`
	ProxyDigest   string `json:"proxy_digest"`
}

func PeerClaimsExtensionOID() asn1.ObjectIdentifier {
	return append(asn1.ObjectIdentifier(nil), peerClaimsOID...)
}

// VerifyPeerIdentity rejects an unverified chain, any non-exact workload URI,
// and absent or conflicting signed proxy claims. DNS and IP SANs are ignored.
func VerifyPeerIdentity(state tls.ConnectionState, expected domain.WorkloadIdentity) error {
	claims, err := verifyPeerIdentity(state, expected)
	if err != nil {
		return err
	}
	if claims.ManifestID == "" || claims.ImageDigest == "" || claims.ProxyDigest == "" {
		return errors.New("provider manifest binding missing")
	}
	return nil
}

func verifyPeerIdentity(state tls.ConnectionState, expected domain.WorkloadIdentity) (PeerIdentityClaims, error) {
	if len(state.PeerCertificates) == 0 || len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 || !state.PeerCertificates[0].Equal(state.VerifiedChains[0][0]) {
		return PeerIdentityClaims{}, errors.New("provider trust chain not verified")
	}
	leaf := state.PeerCertificates[0]
	if leaf.IsCA || leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0 || !hasServerAuth(leaf.ExtKeyUsage) || len(leaf.URIs) != 1 {
		return PeerIdentityClaims{}, errors.New("provider certificate violates gateway policy")
	}
	exact, err := expected.SPIFFEID(leaf.URIs[0].Host)
	if err != nil || leaf.URIs[0].String() != exact.String() {
		return PeerIdentityClaims{}, errors.New("provider workload URI mismatch")
	}
	var raw []byte
	for _, extension := range leaf.Extensions {
		if extension.Id.Equal(peerClaimsOID) {
			if raw != nil {
				return PeerIdentityClaims{}, errors.New("provider claims extension invalid")
			}
			raw = extension.Value
		}
	}
	if len(raw) == 0 {
		return PeerIdentityClaims{}, errors.New("provider claims extension missing")
	}
	var claims PeerIdentityClaims
	decoder := json.NewDecoder(bytesReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&claims); err != nil {
		return PeerIdentityClaims{}, fmt.Errorf("decode provider claims: %w", err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return PeerIdentityClaims{}, errors.New("provider claims have trailing data")
	}
	if claims.PodUID != expected.PodUID() || claims.EndpointEpoch != expected.EndpointEpoch() || claims.RecoveryEpoch != expected.RecoveryEpoch() {
		return PeerIdentityClaims{}, errors.New("provider signed claims mismatch")
	}
	return claims, nil
}

func hasServerAuth(usages []x509.ExtKeyUsage) bool {
	for _, usage := range usages {
		if usage == x509.ExtKeyUsageServerAuth {
			return true
		}
	}
	return false
}

type sliceReader struct{ value []byte }

func bytesReader(value []byte) *sliceReader { return &sliceReader{value: value} }
func (r *sliceReader) Read(p []byte) (int, error) {
	if len(r.value) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.value)
	r.value = r.value[n:]
	return n, nil
}
