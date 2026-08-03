package domain

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestScheduleCommandNoKeyStillRequiresDigests(t *testing.T) {
	digest, err := NewRequestDigestCandidate(2, bytes.Repeat([]byte{2}, 32))
	if err != nil {
		t.Fatal(err)
	}
	valid := ScheduleParams{RequestID: "req-1", AttemptID: "att-1", Tenant: "tenant-1", Model: "model-1", SlotCost: 1, Features: EmptyFeatureSet(), ExecutionBudget: time.Second, DigestCandidates: []RequestDigestCandidate{digest}, DigestWriteVersion: 2}
	command, err := NewScheduleCommand(valid)
	if err != nil {
		t.Fatalf("no-key command rejected: %v", err)
	}
	if command.HasIdempotencyKey() || len(command.DigestCandidates()) != 1 {
		t.Fatal("no-key candidate shape changed")
	}
	missing := valid
	missing.DigestCandidates = nil
	missing.DigestWriteVersion = 0
	if _, err := NewScheduleCommand(missing); err == nil {
		t.Fatal("no-key command without digest candidates accepted")
	}
	mutated := command.DigestCandidates()
	mutated[0] = RequestDigestCandidate{}
	if command.DigestCandidates()[0].Version() != 2 {
		t.Fatal("digest accessor exposed backing slice")
	}
}

func TestScheduleCommandRejectsUnsortedAndUnknownFeatures(t *testing.T) {
	d1, _ := NewRequestDigestCandidate(1, bytes.Repeat([]byte{1}, 32))
	d2, _ := NewRequestDigestCandidate(2, bytes.Repeat([]byte{2}, 32))
	params := ScheduleParams{RequestID: "req", AttemptID: "att", Tenant: "tenant", Model: "model", SlotCost: 1, Features: EmptyFeatureSet(), ExecutionBudget: time.Second, DigestCandidates: []RequestDigestCandidate{d2, d1}, DigestWriteVersion: 2}
	if _, err := NewScheduleCommand(params); err == nil {
		t.Fatal("unsorted digest versions accepted")
	}
	params.DigestCandidates = []RequestDigestCandidate{d1}
	params.DigestWriteVersion = 1
	params.Features = FeatureSet{version: 99}
	if _, err := NewScheduleCommand(params); err == nil {
		t.Fatal("unknown feature version accepted")
	}
}

func TestReservationRefCopiesAndRedactsCapability(t *testing.T) {
	capability := bytes.Repeat([]byte("s"), 32)
	ref, err := NewReservationRefFromParams(ReservationRefParams{ID: "reservation-1", Generation: 1, Capability: capability})
	if err != nil {
		t.Fatal(err)
	}
	capability[0] = 'X'
	copyBytes := ref.Capability()
	copyBytes[1] = 'Y'
	if ref.Capability()[0] != 's' || ref.Capability()[1] != 's' {
		t.Fatal("reservation capability changed through alias")
	}
	if strings.Contains(fmt.Sprintf("%v %#v", ref, ref), strings.Repeat("s", 8)) {
		t.Fatal("reservation formatting exposed capability")
	}
}

func TestSnapshotAndCatalogCopyCacheHints(t *testing.T) {
	asOf := time.Unix(1000, 0).UTC()
	identity, _ := NewWorkloadIdentity(WorkloadIdentityParams{Cluster: "cluster", Namespace: "ns", LogicalEngine: "engine", PodUID: "pod", EndpointEpoch: 1, RecoveryEpoch: 1})
	endpoint, _ := NewEndpointRef("https://engine.test")
	model, _ := NewModelKey("model")
	fingerprint, _ := NewModelFingerprint(model, "revision")
	structuralSource, _ := NewSourceStamp(SourceStructural, 1, 1)
	healthSource, _ := NewSourceStamp(SourceRuntimeHealth, 1, 1)
	structural, _ := NewStoredSourceStamp(StoredSourceStampParams{Source: structuralSource, Identity: identity, Version: 1, AcceptedAt: asOf.Add(-time.Minute), ExpiresAt: asOf.Add(time.Minute)})
	health, _ := NewStoredSourceStamp(StoredSourceStampParams{Source: healthSource, Identity: identity, Version: 1, AcceptedAt: asOf.Add(-time.Minute), ExpiresAt: asOf.Add(time.Minute)})
	hint, _ := NewCacheHint(CacheHintParams{Identity: identity, Digest: [32]byte{1}, ExpiresAt: asOf.Add(time.Minute)})
	hints := []CacheHint{hint}
	snapshot, err := NewInstanceSnapshot(SnapshotParams{Identity: identity, Endpoint: endpoint, Model: fingerprint, Capabilities: EmptyFeatureSet(), Structural: structural, Health: health, HealthState: HealthStateHealthy, DrainState: DrainStateReady, ConfiguredSlots: 2, CacheHints: hints, ProjectionVersion: 1, CatalogAsOf: asOf})
	if err != nil {
		t.Fatal(err)
	}
	hints[0] = CacheHint{}
	accessorHints := snapshot.CacheHints()
	accessorHints[0] = CacheHint{}
	if snapshot.CacheHints()[0].Digest() == ([32]byte{}) {
		t.Fatal("snapshot cache hints changed through alias")
	}
	catalog, err := NewCandidateCatalog([]InstanceSnapshot{snapshot}, asOf)
	if err != nil {
		t.Fatal(err)
	}
	candidates := catalog.Candidates()
	candidates[0].cacheHints[0] = CacheHint{}
	if catalog.Candidates()[0].CacheHints()[0].Digest() == ([32]byte{}) {
		t.Fatal("catalog changed through nested alias")
	}
}
