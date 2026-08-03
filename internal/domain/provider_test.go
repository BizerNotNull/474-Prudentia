package domain

import (
	"strings"
	"testing"
	"time"
)

func validManifestParams() CapabilityManifestParams {
	now := time.Unix(1_700_000_000, 0).UTC()
	return CapabilityManifestParams{
		ID: "vllm-pinned-v1", SchemaVersion: 1, CapabilityVersion: 1, SignatureVersion: 1,
		SignatureVerified: true, VerifiedAt: now, ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour),
		ImageDigest: "sha256:" + strings.Repeat("a", 64), ProxyDigest: "sha256:" + strings.Repeat("b", 64),
		Routes: []string{"/v1/chat/completions", "/health"}, Fields: []string{"model", "messages"},
		Parser: "openai-sse-v1", IdentityProfile: IdentityExactWorkloadMTLS, APCIsolation: APCDisabled,
	}
}

func TestCapabilityManifestRejectsUnknownVersionsAndInvalidSignature(t *testing.T) {
	cases := []func(*CapabilityManifestParams){
		func(p *CapabilityManifestParams) { p.SchemaVersion = 2 },
		func(p *CapabilityManifestParams) { p.CapabilityVersion = 2 },
		func(p *CapabilityManifestParams) { p.SignatureVersion = 2 },
		func(p *CapabilityManifestParams) { p.SignatureVerified = false },
	}
	for _, mutate := range cases {
		p := validManifestParams()
		mutate(&p)
		if _, err := NewCapabilityManifest(p); err == nil {
			t.Fatal("invalid signed manifest accepted")
		}
	}
}

func TestCapabilityManifestCopiesCollectionsAndFailsClosed(t *testing.T) {
	p := validManifestParams()
	m, err := NewCapabilityManifest(p)
	if err != nil {
		t.Fatal(err)
	}
	p.Routes[0] = "/changed"
	routes := m.Routes()
	routes[0] = "/also-changed"
	if m.Routes()[0] != "/v1/chat/completions" {
		t.Fatal("routes were not copied")
	}
	if m.Supports(ProviderCapability(255)) || m.Supports(CapabilityTermination) {
		t.Fatal("unsupported capability enabled")
	}
}

func TestAPCRequiresDedicatedEngineOrProvenTenantSalt(t *testing.T) {
	p := validManifestParams()
	p.APCIsolation = APCTenantSalted
	p.TenantSaltVersion = 1
	if _, err := NewCapabilityManifest(p); err == nil {
		t.Fatal("unproven tenant salt accepted")
	}
	p.TenantSaltProven = true
	m, err := NewCapabilityManifest(p)
	if err != nil {
		t.Fatal(err)
	}
	if !m.Supports(CapabilityAPC) {
		t.Fatal("proven tenant salt not enabled")
	}
	p = validManifestParams()
	p.APCIsolation = APCTenantDedicated
	m, err = NewCapabilityManifest(p)
	if err != nil || !m.Supports(CapabilityAPC) {
		t.Fatal("tenant-dedicated APC rejected")
	}
}

func TestCapabilityManifestCompatibilityIncludesExactRevision(t *testing.T) {
	firstParams := validManifestParams()
	firstParams.ManifestRevision = 1
	first, err := NewCapabilityManifest(firstParams)
	if err != nil {
		t.Fatal(err)
	}
	secondParams := validManifestParams()
	secondParams.ManifestRevision = 2
	second, err := NewCapabilityManifest(secondParams)
	if err != nil {
		t.Fatal(err)
	}
	if first.Compatible(second) || first.PayloadDigestString() == second.PayloadDigestString() {
		t.Fatal("provider manifest revision was ignored")
	}
}
