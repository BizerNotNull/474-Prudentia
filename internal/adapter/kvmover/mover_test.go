package kvmover

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type fakeControl struct {
	status                  []StatusResponse
	starts, commits, aborts int
	opaque                  []byte
	identity                string
	abortDeadline           bool
}

func (f *fakeControl) ConnectorIdentity() string { return f.identity }
func (f *fakeControl) Start(context.Context, StartRequest) (StartResponse, error) {
	f.starts++
	return StartResponse{Opaque: append([]byte(nil), f.opaque...)}, nil
}
func (f *fakeControl) Status(context.Context, StatusRequest) (StatusResponse, error) {
	if len(f.status) == 0 {
		return StatusResponse{}, errors.New("no status")
	}
	next := f.status[0]
	f.status = f.status[1:]
	return next, nil
}
func (f *fakeControl) Commit(context.Context, HandleRequest) error { f.commits++; return nil }
func (f *fakeControl) Abort(ctx context.Context, _ HandleRequest) error {
	f.aborts++
	_, f.abortDeadline = ctx.Deadline()
	return nil
}

type fakeRevalidator struct {
	err   error
	calls int
}

func (r *fakeRevalidator) RevalidateDestination(context.Context, domain.WorkloadIdentity, domain.CapabilityManifest) error {
	r.calls++
	return r.err
}

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

func moverConnector(t *testing.T, now time.Time) domain.ConnectorManifest {
	t.Helper()
	digest := "sha256:" + strings.Repeat("c", 64)
	manifest, err := domain.NewConnectorManifest(domain.ConnectorManifestParams{Digest: digest, ControlPlaneIdentity: "spiffe://trust/mover", SignatureVerified: true, ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), MaxBytes: 1024, MaxOperation: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func moverCacheIdentity(t *testing.T, manifest domain.CapabilityManifest, connector domain.ConnectorManifest) domain.CacheIdentity {
	t.Helper()
	digest := "sha256:" + strings.Repeat("a", 64)
	h := make([]byte, 32)
	id, err := domain.NewCacheIdentity(domain.CacheIdentityParams{Tenant: "tenant", HMACVersion: 1, TenantHMAC: h, ProviderImageDigest: digest, ProxyDigest: digest, ManifestID: "manifest", ProviderManifestDigest: manifest.PayloadDigestString(), ConnectorManifestDigest: connector.Digest(), ManifestSchemaVersion: 1, CapabilityVersion: 1, ModelFingerprint: digest, TokenizerFingerprint: digest, ModelConfigDigest: digest, CacheFormatVersion: 1, CacheContentVersion: 1, AttentionBackend: "flash", DType: "fp16", Quantization: "none", TensorParallel: 1, DataParallel: 1, GPUArchitecture: "gfx", DriverVersion: "1", CacheLayout: "paged", BlockSize: 16})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func moverSpec(t *testing.T, now time.Time) domain.TransferSpec {
	t.Helper()
	manifest := moverManifest(t, now)
	connector := moverConnector(t, now)
	spec, err := domain.NewTransferSpec(domain.TransferSpecParams{Tenant: "tenant", RequestID: "request", CacheIdentity: moverCacheIdentity(t, manifest, connector), Source: moverIdentity(t, "source"), Destination: moverIdentity(t, "destination"), SourceManifest: manifest, DestinationManifest: manifest, ConnectorManifest: connector, ExpiresAt: now.Add(time.Minute), MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

func boundStatus(handle domain.MoverHandle, state domain.TransferState, sequence, bytes uint64) StatusResponse {
	return StatusResponse{State: state, Sequence: sequence, TransferredBytes: bytes, Tenant: handle.Tenant(), RequestID: handle.RequestID(), Source: handle.Source(), Destination: handle.Destination(), ProviderManifestDigest: handle.CacheIdentity().ProviderManifestDigest(), ConnectorManifestDigest: handle.ConnectorManifestDigest()}
}

func TestMoverExactBindingRevalidationAndLifecycle(t *testing.T) {
	now := time.Now()
	control := &fakeControl{opaque: []byte("opaque-ticket"), identity: "spiffe://trust/mover"}
	revalidator := &fakeRevalidator{}
	mover, err := New(control, revalidator, func() time.Time { return now }, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := mover.Start(context.Background(), moverSpec(t, now))
	if err != nil {
		t.Fatal(err)
	}
	control.status = []StatusResponse{boundStatus(handle, domain.TransferRunning, 1, 10), boundStatus(handle, domain.TransferComplete, 2, 100)}
	if _, err = mover.Status(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if err = mover.Commit(context.Background(), handle); err == nil {
		t.Fatal("incomplete transfer committed")
	}
	if _, err = mover.Status(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if err = mover.Commit(context.Background(), handle); err != nil || revalidator.calls != 1 {
		t.Fatal("complete transfer was not revalidated and committed")
	}
	if err = mover.Commit(context.Background(), handle); err != nil || control.commits != 1 {
		t.Fatal("commit was not idempotent")
	}
	if err = mover.Abort(context.Background(), handle); err != nil || control.aborts != 0 {
		t.Fatal("committed handle aborted")
	}
}

func TestMoverRejectsByteCeilingCollisionAndManifestMismatch(t *testing.T) {
	now := time.Now()
	control := &fakeControl{opaque: []byte("reused"), identity: "spiffe://trust/mover"}
	revalidator := &fakeRevalidator{}
	mover, _ := NewBounded(control, revalidator, func() time.Time { return now }, time.Second, 1)
	handle, err := mover.Start(context.Background(), moverSpec(t, now))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = mover.Start(context.Background(), moverSpec(t, now)); err == nil {
		t.Fatal("opaque handle collision overwrote live state")
	}
	control.status = []StatusResponse{boundStatus(handle, domain.TransferComplete, 1, handle.MaxBytes()+1)}
	if _, err = mover.Status(context.Background(), handle); err == nil {
		t.Fatal("transfer byte ceiling was advisory")
	}
	control.status = []StatusResponse{boundStatus(handle, domain.TransferComplete, 2, 10)}
	control.status[0].ConnectorManifestDigest = "sha256:" + strings.Repeat("f", 64)
	if _, err = mover.Status(context.Background(), handle); err == nil {
		t.Fatal("connector manifest mismatch accepted")
	}
}

func TestMoverDestinationMismatchBlocksPublishAndAbortIsBounded(t *testing.T) {
	now := time.Now()
	control := &fakeControl{opaque: []byte("abortable"), identity: "spiffe://trust/mover"}
	revalidator := &fakeRevalidator{err: errors.New("replacement")}
	mover, _ := New(control, revalidator, func() time.Time { return now }, time.Second)
	handle, err := mover.Start(context.Background(), moverSpec(t, now))
	if err != nil {
		t.Fatal(err)
	}
	control.status = []StatusResponse{boundStatus(handle, domain.TransferComplete, 1, 10)}
	if _, err = mover.Status(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if err = mover.Commit(context.Background(), handle); err == nil || control.commits != 0 {
		t.Fatal("destination replacement published transferred state")
	}
	if err = mover.Abort(context.Background(), handle); err != nil || control.aborts != 1 || !control.abortDeadline {
		t.Fatal("abort cleanup was not bounded")
	}
	if err = mover.Abort(context.Background(), handle); err != nil || control.aborts != 1 {
		t.Fatal("abort was not idempotent")
	}
}

func TestConnectorManifestRequiresSeparateSignatureAndPin(t *testing.T) {
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now().UTC()
	payload, err := json.Marshal(connectorManifestPayload{SchemaVersion: 1, ControlPlaneIdentity: "spiffe://trust/mover", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Minute), MaxBytes: 1024, MaxOperation: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	pin := hex.EncodeToString(digest[:])
	verifier, err := NewConnectorManifestVerifier(map[string]ed25519.PublicKey{"connector-signer": public}, []string{pin}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	signed := SignedConnectorManifest{KeyID: "connector-signer", PinnedDigest: pin, Payload: payload, Signature: ed25519.Sign(private, payload)}
	manifest, err := verifier.Verify(signed)
	if err != nil || manifest.ControlPlaneIdentity() != "spiffe://trust/mover" {
		t.Fatal("valid separately signed connector manifest rejected")
	}
	signed.Payload = append([]byte(nil), payload...)
	signed.Payload[len(signed.Payload)-1] ^= 1
	if _, err = verifier.Verify(signed); err == nil {
		t.Fatal("tampered connector manifest accepted")
	}
}
