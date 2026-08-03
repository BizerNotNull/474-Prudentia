package kvmover

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type fakeControl struct {
	status                  []StatusResponse
	starts, commits, aborts int
}

func (f *fakeControl) Start(context.Context, StartRequest) (StartResponse, error) {
	f.starts++
	return StartResponse{Opaque: []byte("opaque-ticket")}, nil
}
func (f *fakeControl) Status(context.Context, StatusRequest) (StatusResponse, error) {
	next := f.status[0]
	f.status = f.status[1:]
	return next, nil
}
func (f *fakeControl) Commit(context.Context, HandleRequest) error { f.commits++; return nil }
func (f *fakeControl) Abort(context.Context, HandleRequest) error  { f.aborts++; return nil }

func moverIdentity(t *testing.T, pod string) domain.WorkloadIdentity {
	t.Helper()
	id, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: "c", Namespace: "n", LogicalEngine: "e", PodUID: pod, EndpointEpoch: 1, RecoveryEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func moverManifest(t *testing.T, now time.Time) domain.CapabilityManifest {
	t.Helper()
	digest := "sha256:" + strings.Repeat("a", 64)
	manifest, err := domain.NewCapabilityManifest(domain.CapabilityManifestParams{ID: "manifest", SchemaVersion: 1, CapabilityVersion: 1, SignatureVersion: 1, SignatureVerified: true, VerifiedAt: now, ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), ImageDigest: digest, ProxyDigest: digest, Routes: []string{"/v1/chat/completions"}, Fields: []string{"model"}, Parser: "p", IdentityProfile: domain.IdentityExactWorkloadMTLS, APCIsolation: domain.APCDisabled, Mover: true})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}
func moverCacheIdentity(t *testing.T) domain.CacheIdentity {
	t.Helper()
	digest := "sha256:" + strings.Repeat("a", 64)
	h := make([]byte, 32)
	id, err := domain.NewCacheIdentity(domain.CacheIdentityParams{Tenant: "tenant", HMACVersion: 1, TenantHMAC: h, ProviderImageDigest: digest, ProxyDigest: digest, ManifestID: "manifest", ConnectorManifestDigest: digest, ManifestSchemaVersion: 1, CapabilityVersion: 1, ModelFingerprint: digest, TokenizerFingerprint: digest, ModelConfigDigest: digest, CacheFormatVersion: 1, CacheContentVersion: 1, AttentionBackend: "flash", DType: "fp16", Quantization: "none", TensorParallel: 1, DataParallel: 1, GPUArchitecture: "gfx", DriverVersion: "1", CacheLayout: "paged", BlockSize: 16})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func moverSpec(t *testing.T, now time.Time) domain.TransferSpec {
	t.Helper()
	manifest := moverManifest(t, now)
	spec, err := domain.NewTransferSpec(domain.TransferSpecParams{Tenant: "tenant", RequestID: "request", CacheIdentity: moverCacheIdentity(t), Source: moverIdentity(t, "source"), Destination: moverIdentity(t, "destination"), SourceManifest: manifest, DestinationManifest: manifest, ExpiresAt: now.Add(time.Minute), MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

func TestMoverOpaqueMonotonicCommitAndAbort(t *testing.T) {
	now := time.Now()
	destination := moverIdentity(t, "destination")
	control := &fakeControl{status: []StatusResponse{{State: domain.TransferRunning, Sequence: 1, Tenant: "tenant", Destination: destination, ManifestID: "manifest"}, {State: domain.TransferComplete, Sequence: 2, TransferredBytes: 100, Tenant: "tenant", Destination: destination, ManifestID: "manifest"}}}
	mover, err := New(control, func() time.Time { return now }, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := mover.Start(context.Background(), moverSpec(t, now))
	if err != nil {
		t.Fatal(err)
	}
	if string(handle.Opaque()) != "opaque-ticket" {
		t.Fatal("opaque handle changed")
	}
	if _, err = mover.Status(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if err = mover.Commit(context.Background(), handle); err == nil {
		t.Fatal("incomplete transfer committed")
	}
	if _, err = mover.Status(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if err = mover.Commit(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if err = mover.Commit(context.Background(), handle); err != nil || control.commits != 1 {
		t.Fatal("commit was not idempotent")
	}
	if err = mover.Abort(context.Background(), handle); err != nil || control.aborts != 0 {
		t.Fatal("committed handle aborted")
	}
}
