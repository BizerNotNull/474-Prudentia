package domain

import (
	"strings"
	"testing"
	"time"
)

func observationFixture(t *testing.T) (WorkloadRef, PodRef, WorkloadIdentity) {
	t.Helper()
	resource, err := NewResourceRef(ResourceRefParams{Cluster: "c", Namespace: "n", Name: "engine", UID: "workload-uid", ResourceVersion: "11"})
	if err != nil {
		t.Fatal(err)
	}
	workload, err := NewWorkloadRef(WorkloadStatefulSet, resource, 1)
	if err != nil {
		t.Fatal(err)
	}
	podResource, err := NewResourceRef(ResourceRefParams{Cluster: "c", Namespace: "n", Name: "engine-0", UID: "pod-uid", ResourceVersion: "12"})
	if err != nil {
		t.Fatal(err)
	}
	pod, err := NewPodRef(PodRefParams{Resource: podResource, WorkloadUID: "workload-uid"})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := NewWorkloadIdentity(WorkloadIdentityParams{Cluster: "c", Namespace: "n", LogicalEngine: "model", PodUID: "pod-uid", EndpointEpoch: 3, RecoveryEpoch: 4})
	if err != nil {
		t.Fatal(err)
	}
	return workload, pod, identity
}

func TestObservationVariantsAndOptionalFacts(t *testing.T) {
	workload, pod, identity := observationFixture(t)
	fact, err := NewStructuralFact(StructuralFactParams{Endpoint: "https://engine.example", Model: "model", Workload: workload, Members: []PodRef{pod}, EndpointEpoch: 3, RecoveryEpoch: 4})
	if err != nil {
		t.Fatal(err)
	}
	stamp, err := NewSourceStamp(SourceStructural, 2, 7)
	if err != nil {
		t.Fatal(err)
	}
	inputMembers := []PodRef{pod}
	factFromInput, err := NewStructuralFact(StructuralFactParams{Endpoint: "https://engine.example", Model: "model", Workload: workload, Members: inputMembers, EndpointEpoch: 3, RecoveryEpoch: 4})
	if err != nil {
		t.Fatal(err)
	}
	inputMembers[0] = PodRef{}
	if got := factFromInput.Members(); len(got) != 1 || got[0].UID() != "pod-uid" {
		t.Fatalf("membership input was not copied: %#v", got)
	}
	returned := factFromInput.Members()
	returned[0] = PodRef{}
	if factFromInput.Members()[0].UID() != "pod-uid" {
		t.Fatal("membership accessor leaked mutable slice")
	}

	o, err := NewObservation(ObservationParams{Stamp: stamp, Identity: identity, TTLClass: TTLStructural, Structural: fact})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := o.Structural(); !ok {
		t.Fatal("structural fact missing")
	}
	if _, ok := o.RuntimeHealth(); ok {
		t.Fatal("unexpected health fact")
	}
	if _, ok := o.SourceReportedAt(); ok {
		t.Fatal("absent diagnostic time reported present")
	}

	healthStamp, _ := NewSourceStamp(SourceRuntimeHealth, 2, 8)
	health, _ := NewRuntimeHealthFact(HealthReady, true)
	if _, err := NewObservation(ObservationParams{Stamp: healthStamp, Identity: identity, TTLClass: TTLLoad, RuntimeHealth: health}); err == nil {
		t.Fatal("accepted mismatched TTL class")
	}
	if _, err := NewObservation(ObservationParams{Stamp: SourceStamp{}, Identity: identity, TTLClass: TTLStructural, Structural: fact}); err == nil {
		t.Fatal("accepted unknown source kind")
	}
}

func TestLoadFactOptionalUtilization(t *testing.T) {
	load, err := NewLoadFact(LoadFactParams{RunningRequests: 2, QueuedRequests: 1})
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := load.Utilization(); ok || value != 0 {
		t.Fatalf("unexpected optional utilization: %v %v", value, ok)
	}
	load, err = NewLoadFact(LoadFactParams{Utilization: .5, HasUtilization: true})
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := load.Utilization(); !ok || value != .5 {
		t.Fatalf("missing optional utilization: %v %v", value, ok)
	}
	if _, err := NewLoadFact(LoadFactParams{Utilization: 1.1, HasUtilization: true}); err == nil {
		t.Fatal("accepted invalid utilization")
	}
}

func TestStoredStampAndProjectionUpdate(t *testing.T) {
	_, _, identity := observationFixture(t)
	now := time.Unix(100, 0).UTC()
	structuralSource, _ := NewSourceStamp(SourceStructural, 1, 1)
	healthSource, _ := NewSourceStamp(SourceRuntimeHealth, 1, 1)
	structural, _ := NewStoredSourceStamp(StoredSourceStampParams{Source: structuralSource, Identity: identity, Version: 1, AcceptedAt: now, ExpiresAt: now.Add(time.Minute)})
	health, _ := NewStoredSourceStamp(StoredSourceStampParams{Source: healthSource, Identity: identity, Version: 1, AcceptedAt: now, ExpiresAt: now.Add(time.Minute)})
	update, err := NewProjectionUpdate(ProjectionUpdateParams{Identity: identity, Structural: structural, Health: health, ConfiguredSlots: 4, AdmissionLimit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := update.LoadStamp(); ok {
		t.Fatal("absent load stamp reported present")
	}
	if !structural.FreshAt(now) || structural.FreshAt(now.Add(time.Minute)) {
		t.Fatal("freshness boundary is wrong")
	}
	if _, err := NewProjectionUpdate(ProjectionUpdateParams{Identity: identity, Structural: structural, Health: health, ConfiguredSlots: 2, AdmissionLimit: 3}); err == nil {
		t.Fatal("accepted admission above physical slots")
	}
	if _, err := NewProjectionVersion(0); err == nil {
		t.Fatal("accepted zero projection version")
	}
}

func TestObservationBounds(t *testing.T) {
	if _, err := NewSourceSequence(0); err == nil {
		t.Fatal("accepted zero sequence")
	}
	if _, err := NewWriterGeneration(0); err == nil {
		t.Fatal("accepted zero writer generation")
	}
	if _, err := NewResourceRef(ResourceRefParams{Cluster: "c", Namespace: "n", Name: "x", UID: strings.Repeat("u", 129), ResourceVersion: "1"}); err == nil {
		t.Fatal("accepted oversized UID")
	}
}
