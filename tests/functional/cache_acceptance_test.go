package functional_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	cacheapp "github.com/BizerNotNull/474-Prudentia/internal/cache"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

func cacheParams(tenant string, version uint32, hmacByte byte) domain.CacheIdentityParams {
	return domain.CacheIdentityParams{
		Tenant: tenant, HMACVersion: version, TenantHMAC: []byte(strings.Repeat(string(hmacByte), 32)),
		ProviderImageDigest: "sha256:" + strings.Repeat("a", 64), ProxyDigest: "sha256:" + strings.Repeat("b", 64), ManifestID: "manifest-v1",
		ConnectorManifestDigest: "sha256:" + strings.Repeat("c", 64), ManifestSchemaVersion: 1, CapabilityVersion: 1,
		ModelFingerprint: "sha256:" + strings.Repeat("d", 64), TokenizerFingerprint: "sha256:" + strings.Repeat("e", 64), ModelConfigDigest: "sha256:" + strings.Repeat("f", 64),
		CacheFormatVersion: 1, CacheContentVersion: 1, AttentionBackend: "flash", DType: "bf16", Quantization: "none", TensorParallel: 1, DataParallel: 1,
		GPUArchitecture: "sm90", DriverVersion: "550", CacheLayout: "paged", BlockSize: 16,
	}
}
func cacheIdentity(t *testing.T, p domain.CacheIdentityParams) domain.CacheIdentity {
	t.Helper()
	id, err := domain.NewCacheIdentity(p)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func cacheWorkload(t *testing.T, uid string) domain.WorkloadIdentity {
	t.Helper()
	id, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: "c", Namespace: "n", LogicalEngine: "e", PodUID: uid, EndpointEpoch: 1, RecoveryEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func cacheManifest(t *testing.T, now time.Time) domain.CapabilityManifest {
	t.Helper()
	manifest, err := domain.NewCapabilityManifest(domain.CapabilityManifestParams{ID: "manifest-v1", SchemaVersion: 1, CapabilityVersion: 1, SignatureVersion: 1, SignatureVerified: true, VerifiedAt: now, ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), ImageDigest: "sha256:" + strings.Repeat("a", 64), ProxyDigest: "sha256:" + strings.Repeat("b", 64), Routes: []string{"/v1/chat/completions"}, Fields: []string{"model", "messages"}, Parser: "parser-v1", IdentityProfile: domain.IdentityExactWorkloadMTLS, APCIsolation: domain.APCDisabled})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func TestCacheCompatibilityAndRotationFailClosed(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	metadata, err := cacheapp.NewMetadata(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	source := cacheWorkload(t, "pod-a")
	v1 := cacheIdentity(t, cacheParams("tenant-a", 1, 'a'))
	v2 := cacheIdentity(t, cacheParams("tenant-a", 2, 'b'))
	otherTenant := cacheIdentity(t, cacheParams("tenant-b", 2, 'b'))
	if err := metadata.RecordVerified(v1, source, cacheManifest(t, now), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if hit, err := metadata.Lookup(context.Background(), v1); err != nil || !hit.IsHit() {
		t.Fatalf("retained v1 lookup = hit:%v err:%v", hit.IsHit(), err)
	}
	if hit, err := metadata.Lookup(context.Background(), v2); err != nil || hit.IsHit() {
		t.Fatalf("unrecorded v2 lookup = hit:%v err:%v", hit.IsHit(), err)
	}
	if err := metadata.RecordVerified(v2, source, cacheManifest(t, now), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if hit, err := metadata.Lookup(context.Background(), v2); err != nil || !hit.IsHit() {
		t.Fatalf("current v2 lookup = hit:%v err:%v", hit.IsHit(), err)
	}
	if hit, err := metadata.Lookup(context.Background(), otherTenant); err != nil || hit.IsHit() {
		t.Fatalf("cross-tenant lookup = hit:%v err:%v", hit.IsHit(), err)
	}
	now = now.Add(time.Minute + time.Nanosecond)
	if hit, err := metadata.Lookup(context.Background(), v2); err != nil || hit.IsHit() {
		t.Fatalf("expired record must be a miss, hit:%v err:%v", hit.IsHit(), err)
	}
}

func TestCacheOutageFallsBackColdUnlessCompatibilityRequired(t *testing.T) {
	now := time.Unix(2000, 0).UTC()
	identity := cacheIdentity(t, cacheParams("tenant-a", 1, 'a'))
	targetIdentity := cacheWorkload(t, "pod-target")
	target, err := domain.NewReservedTarget("tenant-a", targetIdentity, cacheManifest(t, now), identity)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := cacheapp.NewColdCoordinator(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for name, requirement := range map[string]domain.CacheRequirement{"cold allowed": domain.ColdAllowed, "compatible required": domain.RequireCompatible} {
		t.Run(name, func(t *testing.T) {
			req, err := domain.NewCacheRequest(domain.CacheRequestParams{Tenant: "tenant-a", RequestID: "request-a", Identity: identity, Hint: domain.NewCacheMiss(), Requirement: requirement, Budget: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			preparation, err := coordinator.Prepare(context.Background(), req, target)
			if requirement == domain.RequireCompatible {
				if !errors.Is(err, cacheapp.ErrCompatibleCacheRequired) {
					t.Fatalf("error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if preparation.Kind() != domain.CachePreparationCold {
				t.Fatalf("kind = %d", preparation.Kind())
			}
		})
	}
}
