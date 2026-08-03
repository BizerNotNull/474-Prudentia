package cache

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

func cacheIdentity(t *testing.T, tenant string, h byte) domain.CacheIdentity {
	t.Helper()
	digest := "sha256:" + strings.Repeat("a", 64)
	id, err := domain.NewCacheIdentity(domain.CacheIdentityParams{Tenant: tenant, HMACVersion: 1, TenantHMAC: bytesOf(h, 32), ProviderImageDigest: digest, ProxyDigest: digest, ManifestID: "manifest", ConnectorManifestDigest: digest, ManifestSchemaVersion: 1, CapabilityVersion: 1, ModelFingerprint: digest, TokenizerFingerprint: digest, ModelConfigDigest: digest, CacheFormatVersion: 1, CacheContentVersion: 1, AttentionBackend: "flash", DType: "fp16", Quantization: "none", TensorParallel: 1, DataParallel: 1, GPUArchitecture: "gfx", DriverVersion: "1", CacheLayout: "paged", BlockSize: 16})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func bytesOf(v byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = v
	}
	return out
}
func cacheSource(t *testing.T) domain.WorkloadIdentity {
	t.Helper()
	id, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: "c", Namespace: "n", LogicalEngine: "e", PodUID: "p", EndpointEpoch: 1, RecoveryEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestMetadataDefaultsMissAndExpires(t *testing.T) {
	now := time.Now()
	metadata, err := NewMetadata(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	id := cacheIdentity(t, "tenant-a", 1)
	hint, err := metadata.Lookup(context.Background(), id)
	if err != nil || hint.Kind() != domain.CacheMiss {
		t.Fatal("empty metadata did not miss")
	}
	if err = metadata.RecordVerified(id, cacheSource(t), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	hint, _ = metadata.Lookup(context.Background(), id)
	if !hint.IsHit() {
		t.Fatal("verified compatible record missed")
	}
	now = now.Add(2 * time.Minute)
	hint, _ = metadata.Lookup(context.Background(), id)
	if hint.Kind() != domain.CacheMiss {
		t.Fatal("expired record remained a hit")
	}
}

func TestMetadataIsTenantAndCompatibilityBound(t *testing.T) {
	now := time.Now()
	metadata, _ := NewMetadata(func() time.Time { return now })
	a := cacheIdentity(t, "tenant-a", 3)
	b := cacheIdentity(t, "tenant-b", 3)
	_ = metadata.RecordVerified(a, cacheSource(t), now.Add(time.Minute))
	hint, _ := metadata.Lookup(context.Background(), b)
	if hint.Kind() != domain.CacheMiss {
		t.Fatal("cross-tenant cache hit")
	}
}
