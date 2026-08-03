package cache

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

func metadataManifest(t *testing.T, now time.Time) domain.CapabilityManifest {
	t.Helper()
	digest := "sha256:" + strings.Repeat("a", 64)
	manifest, err := domain.NewCapabilityManifest(domain.CapabilityManifestParams{ID: "manifest", SchemaVersion: 1, CapabilityVersion: 1, SignatureVersion: 1, SignatureVerified: true, VerifiedAt: now, ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), ImageDigest: digest, ProxyDigest: digest, Routes: []string{"/v1/chat/completions"}, Fields: []string{"model"}, Parser: "p", IdentityProfile: domain.IdentityExactWorkloadMTLS, APCIsolation: domain.APCDisabled, CacheMetadata: true})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func cacheIdentity(t *testing.T, tenant string, version uint32, h byte, manifest domain.CapabilityManifest) domain.CacheIdentity {
	t.Helper()
	digest := "sha256:" + strings.Repeat("a", 64)
	id, err := domain.NewCacheIdentity(domain.CacheIdentityParams{Tenant: tenant, HMACVersion: version, TenantHMAC: bytesOf(h, 32), ProviderImageDigest: digest, ProxyDigest: digest, ManifestID: "manifest", ProviderManifestDigest: manifest.PayloadDigestString(), ConnectorManifestDigest: digest, ManifestSchemaVersion: 1, CapabilityVersion: 1, ModelFingerprint: digest, TokenizerFingerprint: digest, ModelConfigDigest: digest, CacheFormatVersion: 1, CacheContentVersion: 1, AttentionBackend: "flash", DType: "fp16", Quantization: "none", TensorParallel: 1, DataParallel: 1, GPUArchitecture: "gfx", DriverVersion: "1", CacheLayout: "paged", BlockSize: 16})
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

func TestIdentityKeyringTenantDomainSeparationRotationAndWriteVersion(t *testing.T) {
	keyring, err := NewIdentityKeyring([]HMACKeyVersion{{Version: 1, Key: bytesOf(1, 32)}, {Version: 2, Key: bytesOf(2, 32)}}, 2)
	if err != nil {
		t.Fatal(err)
	}
	prefix := sha256.Sum256([]byte("same-prefix"))
	a, _ := keyring.Candidates("tenant-a", prefix)
	b, _ := keyring.Candidates("tenant-b", prefix)
	if len(a) != 2 || a[0].Version != 1 || a[1].Version != 2 || a[0].Digest == a[1].Digest || a[0].Digest == b[0].Digest || a[1].Digest == b[1].Digest {
		t.Fatal("retained key versions were not tenant-domain-separated")
	}
	write, err := keyring.WriteCandidate(a)
	if err != nil || write.Version != 2 || write.Digest != a[1].Digest {
		t.Fatal("current write version was not selected")
	}
	if _, err = NewIdentityKeyring([]HMACKeyVersion{{Version: 1, Key: bytesOf(1, 32)}}, 2); err == nil {
		t.Fatal("missing current write version accepted")
	}
}

func TestMetadataRotationTTLAndCorruptMismatchAreMisses(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	manifest := metadataManifest(t, now)
	metadata, err := NewMetadataWithLimits(func() time.Time { return now }, time.Minute, 2)
	if err != nil {
		t.Fatal(err)
	}
	v1 := cacheIdentity(t, "tenant-a", 1, 1, manifest)
	v2 := cacheIdentity(t, "tenant-a", 2, 2, manifest)
	if err = metadata.RecordVerified(v1, cacheSource(t), manifest, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	// v2 is checked before the retained v1 candidate. A miss on the current
	// version must not prevent rotation fallback to retained metadata.
	hint, err := metadata.LookupCandidates(context.Background(), []domain.CacheIdentity{v2, v1})
	if err != nil || !hint.IsHit() {
		t.Fatal("retained cache HMAC version did not resolve")
	}
	now = now.Add(time.Minute)
	hint, _ = metadata.Lookup(context.Background(), v1)
	if hint.Kind() != domain.CacheMiss {
		t.Fatal("TTL equality boundary remained a hit")
	}
	if err = metadata.RecordVerified(v2, cacheSource(t), manifest, now.Add(2*time.Minute)); err == nil {
		t.Fatal("oversize caller TTL accepted")
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	mismatched, mismatchErr := domain.NewCapabilityManifest(domain.CapabilityManifestParams{ID: "manifest", SchemaVersion: 1, CapabilityVersion: 1, SignatureVersion: 1, SignatureVerified: true, VerifiedAt: now, ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), ImageDigest: digest, ProxyDigest: digest, Routes: []string{"/v1/chat/completions"}, Fields: []string{"model"}, Parser: "different-revision", IdentityProfile: domain.IdentityExactWorkloadMTLS, APCIsolation: domain.APCDisabled, CacheMetadata: true})
	if mismatchErr != nil {
		t.Fatal(mismatchErr)
	}
	if err = metadata.RecordVerified(v2, cacheSource(t), mismatched, now.Add(30*time.Second)); err == nil {
		t.Fatal("mismatched signed manifest revision accepted")
	}
}

func TestMetadataTenantIsolationAndBoundedStorage(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	manifest := metadataManifest(t, now)
	metadata, _ := NewMetadataWithLimits(func() time.Time { return now }, time.Minute, 1)
	a := cacheIdentity(t, "tenant-a", 1, 3, manifest)
	b := cacheIdentity(t, "tenant-b", 1, 3, manifest)
	if err := metadata.RecordVerified(a, cacheSource(t), manifest, now.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	hint, _ := metadata.Lookup(context.Background(), b)
	if hint.Kind() != domain.CacheMiss {
		t.Fatal("cross-tenant cache hit")
	}
	if err := metadata.RecordVerified(b, cacheSource(t), manifest, now.Add(30*time.Second)); err == nil {
		t.Fatal("bounded metadata storage grew without eviction")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	hint, err := metadata.Lookup(canceled, a)
	if err != nil || hint.Kind() != domain.CacheMiss {
		t.Fatal("metadata outage/cancellation did not fail closed to miss")
	}
}
