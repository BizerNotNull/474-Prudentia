package domain

import (
	"strings"
	"testing"
	"time"
)

func cacheIdentityParams(tenant string) CacheIdentityParams {
	return CacheIdentityParams{Tenant: tenant, HMACVersion: 1, TenantHMAC: []byte(strings.Repeat("h", 32)), ProviderImageDigest: "sha256:" + strings.Repeat("a", 64), ProxyDigest: "sha256:" + strings.Repeat("b", 64), ManifestID: "vllm-pinned-v1", ProviderManifestDigest: "sha256:" + strings.Repeat("9", 64), ConnectorManifestDigest: "sha256:" + strings.Repeat("c", 64), ManifestSchemaVersion: 1, CapabilityVersion: 1, ModelFingerprint: "sha256:" + strings.Repeat("d", 64), TokenizerFingerprint: "sha256:" + strings.Repeat("e", 64), ModelConfigDigest: "sha256:" + strings.Repeat("f", 64), CacheFormatVersion: 1, CacheContentVersion: 1, AttentionBackend: "flash-attention", DType: "bf16", Quantization: "none", TensorParallel: 2, DataParallel: 1, GPUArchitecture: "sm90", DriverVersion: "550.54", CacheLayout: "paged-v1", BlockSize: 16}
}
func testWorkload(t *testing.T, uid string, epoch uint64) WorkloadIdentity {
	t.Helper()
	i, err := NewWorkloadIdentity(WorkloadIdentityParams{Cluster: "c", Namespace: "n", LogicalEngine: "e", PodUID: uid, EndpointEpoch: epoch, RecoveryEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	return i
}

func TestCacheIdentityRequiresTenantPrivateHMACAndEveryDimension(t *testing.T) {
	p := cacheIdentityParams("tenant-a")
	id, err := NewCacheIdentity(p)
	if err != nil {
		t.Fatal(err)
	}
	p.TenantHMAC[0] = 'x'
	if id.TenantHMAC()[0] == 'x' {
		t.Fatal("HMAC input was not copied")
	}
	other := cacheIdentityParams("tenant-b")
	otherID, err := NewCacheIdentity(other)
	if err != nil {
		t.Fatal(err)
	}
	if id.Compatible(otherID) {
		t.Fatal("cache compatibility crossed tenant")
	}
	changed := cacheIdentityParams("tenant-a")
	changed.DriverVersion = "551"
	changedID, _ := NewCacheIdentity(changed)
	if id.Compatible(changedID) {
		t.Fatal("cache compatibility ignored driver version")
	}
}

func TestCacheIdentityRejectsMismatchInEveryCompatibilityDimension(t *testing.T) {
	baseParams := cacheIdentityParams("tenant-a")
	base, err := NewCacheIdentity(baseParams)
	if err != nil {
		t.Fatal(err)
	}
	otherDigest := "sha256:" + strings.Repeat("1", 64)
	mutations := []func(*CacheIdentityParams){
		func(p *CacheIdentityParams) { p.HMACVersion++ },
		func(p *CacheIdentityParams) { p.TenantHMAC = []byte(strings.Repeat("x", 32)) },
		func(p *CacheIdentityParams) { p.ProviderImageDigest = otherDigest },
		func(p *CacheIdentityParams) { p.ProxyDigest = otherDigest },
		func(p *CacheIdentityParams) { p.ManifestID = "revision-two" },
		func(p *CacheIdentityParams) { p.ProviderManifestDigest = otherDigest },
		func(p *CacheIdentityParams) { p.ConnectorManifestDigest = otherDigest },
		func(p *CacheIdentityParams) { p.ModelFingerprint = otherDigest },
		func(p *CacheIdentityParams) { p.TokenizerFingerprint = otherDigest },
		func(p *CacheIdentityParams) { p.ModelConfigDigest = otherDigest },
		func(p *CacheIdentityParams) { p.CacheFormatVersion++ },
		func(p *CacheIdentityParams) { p.CacheContentVersion++ },
		func(p *CacheIdentityParams) { p.AttentionBackend = "other" },
		func(p *CacheIdentityParams) { p.DType = "fp16" },
		func(p *CacheIdentityParams) { p.Quantization = "int8" },
		func(p *CacheIdentityParams) { p.TensorParallel++ },
		func(p *CacheIdentityParams) { p.DataParallel++ },
		func(p *CacheIdentityParams) { p.GPUArchitecture = "sm80" },
		func(p *CacheIdentityParams) { p.DriverVersion = "551" },
		func(p *CacheIdentityParams) { p.CacheLayout = "contiguous" },
		func(p *CacheIdentityParams) { p.BlockSize++ },
	}
	for index, mutate := range mutations {
		params := cacheIdentityParams("tenant-a")
		mutate(&params)
		changed, changedErr := NewCacheIdentity(params)
		if changedErr != nil {
			t.Fatalf("mutation %d invalid fixture: %v", index, changedErr)
		}
		if base.Compatible(changed) {
			t.Fatalf("compatibility dimension %d was ignored", index)
		}
	}
}

func TestCacheHitIsTruthfulAndTenantCompatible(t *testing.T) {
	id, _ := NewCacheIdentity(cacheIdentityParams("tenant-a"))
	other, _ := NewCacheIdentity(cacheIdentityParams("tenant-b"))
	source := testWorkload(t, "pod-a", 1)
	if _, err := NewCacheHit(id, other, source, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("cross-tenant hit accepted")
	}
	hit, err := NewCacheHit(id, id, source, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if !hit.ValidFor(id, time.Now()) || hit.ValidFor(id, time.Now().Add(2*time.Hour)) {
		t.Fatal("hit validity is untruthful")
	}
	if NewCacheMiss().IsHit() {
		t.Fatal("miss reported as hit")
	}
}

func TestMoverHandleIsCopiedBoundedAndRedacted(t *testing.T) {
	p := validManifestParams()
	p.Mover = true
	m, err := NewCapabilityManifest(p)
	if err != nil {
		t.Fatal(err)
	}
	params := cacheIdentityParams("tenant-a")
	params.ProviderManifestDigest = m.PayloadDigestString()
	id, _ := NewCacheIdentity(params)
	src := testWorkload(t, "pod-a", 1)
	dst := testWorkload(t, "pod-b", 1)
	connector, _ := NewConnectorManifest(ConnectorManifestParams{Digest: params.ConnectorManifestDigest, ControlPlaneIdentity: "spiffe://trust/connector", SignatureVerified: true, ValidFrom: time.Now().Add(-time.Hour), ValidUntil: time.Now().Add(time.Hour), MaxBytes: 1024, MaxOperation: time.Second})
	spec, err := NewTransferSpec(TransferSpecParams{Tenant: "tenant-a", RequestID: "request-a", CacheIdentity: id, Source: src, Destination: dst, SourceManifest: m, DestinationManifest: m, ConnectorManifest: connector, ExpiresAt: time.Now().Add(time.Minute), MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte("opaque-control-token")
	h, err := NewMoverHandle(spec, secret)
	if err != nil {
		t.Fatal(err)
	}
	secret[0] = 'X'
	out := h.Opaque()
	out[0] = 'Y'
	if string(h.Opaque()) != "opaque-control-token" {
		t.Fatal("opaque handle was not copied")
	}
	if strings.Contains(h.String(), "opaque-control-token") {
		t.Fatal("handle leaked in String")
	}
	if _, err := NewMoverHandle(spec, make([]byte, MaxMoverHandleBytes+1)); err == nil {
		t.Fatal("oversize handle accepted")
	}
}

func TestTransferStatusIsMonotonic(t *testing.T) {
	p := validManifestParams()
	p.Mover = true
	m, _ := NewCapabilityManifest(p)
	params := cacheIdentityParams("tenant-a")
	params.ProviderManifestDigest = m.PayloadDigestString()
	id, _ := NewCacheIdentity(params)
	src := testWorkload(t, "pod-a", 1)
	dst := testWorkload(t, "pod-b", 1)
	connector, _ := NewConnectorManifest(ConnectorManifestParams{Digest: params.ConnectorManifestDigest, ControlPlaneIdentity: "spiffe://trust/connector", SignatureVerified: true, ValidFrom: time.Now().Add(-time.Hour), ValidUntil: time.Now().Add(time.Hour), MaxBytes: 1024, MaxOperation: time.Second})
	spec, _ := NewTransferSpec(TransferSpecParams{Tenant: "tenant-a", RequestID: "request-a", CacheIdentity: id, Source: src, Destination: dst, SourceManifest: m, DestinationManifest: m, ConnectorManifest: connector, ExpiresAt: time.Now().Add(time.Minute), MaxBytes: 1024})
	h, _ := NewMoverHandle(spec, []byte("token"))
	pending, err := NewTransferStatus(TransferStatusParams{Handle: h, State: TransferPending, Sequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	running, err := NewTransferStatus(TransferStatusParams{Handle: h, State: TransferRunning, Sequence: 2, TransferredBytes: 64, Previous: pending, HasPrevious: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewTransferStatus(TransferStatusParams{Handle: h, State: TransferPending, Sequence: 3, TransferredBytes: 32, Previous: running, HasPrevious: true}); err == nil {
		t.Fatal("regressing transfer accepted")
	}
	complete, err := NewTransferStatus(TransferStatusParams{Handle: h, State: TransferComplete, Sequence: 3, TransferredBytes: 128, Previous: running, HasPrevious: true})
	if err != nil {
		t.Fatal(err)
	}
	if complete.CanAdvanceTo(running) {
		t.Fatal("terminal transfer advanced")
	}
}
