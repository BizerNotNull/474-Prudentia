package controller

import (
	"context"
	"errors"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"reflect"
	"sync"
	"testing"
	"time"
)

type fakeCatalog struct {
	mu           sync.Mutex
	events       []string
	states       []domain.ResourceState
	observations []domain.Observation
	updates      []domain.ProjectionUpdate
}

func (f *fakeCatalog) AcquireControllerWriterGeneration(context.Context, string, string) (domain.WriterGeneration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "acquire")
	return 7, nil
}

func (f *fakeCatalog) ReplaceResourceProjection(_ context.Context, generation domain.WriterGeneration, state domain.ResourceState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "replace")
	f.states = append(f.states, state)
	if generation != 7 {
		return domain.ErrStaleWriterGeneration
	}
	return nil
}

func (f *fakeCatalog) RecordObservation(_ context.Context, _ domain.WriterGeneration, observation domain.Observation) (domain.StoredSourceStamp, bool, error) {
	f.observations = append(f.observations, observation)
	stamp, err := domain.NewStoredSourceStamp(domain.StoredSourceStampParams{
		Source: observation.Stamp(), Identity: observation.Identity(),
		Version: observation.Stamp().Sequence().Uint64(), AcceptedAt: time.Unix(100, 0).UTC(), ExpiresAt: time.Unix(200, 0).UTC(),
	})
	return stamp, err == nil, err
}
func (f *fakeCatalog) SyncCapacityProjection(_ context.Context, _ domain.WriterGeneration, update domain.ProjectionUpdate) (domain.ProjectionVersion, error) {
	f.updates = append(f.updates, update)
	return 1, nil
}
func (f *fakeCatalog) ListIncompleteWorkloadOperations(context.Context, domain.WriterGeneration) ([]domain.WorkloadOperation, error) {
	return nil, nil
}
func (f *fakeCatalog) AdvanceWorkloadOperationFence(context.Context, domain.WriterGeneration, domain.WorkloadRef, domain.WorkloadOperationIntent) (domain.WorkloadOperation, error) {
	return domain.WorkloadOperation{}, nil
}
func (f *fakeCatalog) RecordWorkloadBarrier(context.Context, domain.WriterGeneration, domain.WorkloadBarrierProof) error {
	return nil
}
func (f *fakeCatalog) WaitForOldCallsQuiescent(context.Context, domain.WriterGeneration, domain.WorkloadOperationRef) error {
	return nil
}
func (f *fakeCatalog) RecordWorkloadVictims(context.Context, domain.WriterGeneration, domain.WorkloadVictimObservation) error {
	return nil
}
func (f *fakeCatalog) CompleteWorkloadOperationAndReopen(context.Context, domain.WriterGeneration, domain.WorkloadCompletionProof) error {
	return nil
}

type fakeDiscovery struct {
	key          domain.ResourceKey
	state        domain.ResourceState
	observations []domain.Observation
}

func (f *fakeDiscovery) Run(ctx context.Context, _ func(domain.ResourceKey)) error {
	<-ctx.Done()
	return nil
}
func (f *fakeDiscovery) WaitForSync(context.Context) error { return nil }
func (f *fakeDiscovery) ListKeys(context.Context) ([]domain.ResourceKey, error) {
	return []domain.ResourceKey{f.key}, nil
}
func (f *fakeDiscovery) Reconcile(context.Context, domain.ResourceKey) (domain.ResourceState, error) {
	return f.state, nil
}
func (f *fakeDiscovery) ReadDesired(context.Context, domain.ResourceKey) (domain.DesiredModel, error) {
	return domain.DesiredModel{}, nil
}
func (f *fakeDiscovery) ReconcileDiscovery(context.Context, domain.ResourceKey, domain.WriterGeneration) ([]domain.Observation, error) {
	return append([]domain.Observation(nil), f.observations...), nil
}
func (f *fakeDiscovery) ApplyDesired(context.Context, domain.DesiredModel) (domain.ApplyResult, error) {
	return domain.ApplyResult{}, nil
}
func (f *fakeDiscovery) InstallWorkloadOperationBarrier(context.Context, domain.WorkloadOperation, domain.WorkloadRef, []domain.PodRef) (domain.WorkloadBarrierProof, error) {
	return domain.WorkloadBarrierProof{}, nil
}
func (f *fakeDiscovery) ObserveWorkloadVictims(context.Context, domain.WorkloadOperationRef, domain.PodUIDSet) (domain.WorkloadVictimObservation, error) {
	return domain.WorkloadVictimObservation{}, nil
}
func (f *fakeDiscovery) BuildWorkloadCompletionProof(context.Context, domain.WorkloadBarrierProof, domain.WorkloadVictimObservation) (domain.WorkloadCompletionProof, error) {
	return domain.WorkloadCompletionProof{}, nil
}

type fakeProvider struct {
	health domain.RuntimeHealthObservation
	load   domain.LoadObservation
	err    error
}

func (fakeProvider) ProbeTarget(context.Context, domain.BackendProjection) (domain.ProbeTarget, error) {
	return domain.ProbeTarget{}, nil
}
func (f fakeProvider) Probe(context.Context, domain.ProbeTarget) (domain.RuntimeHealthObservation, error) {
	return f.health, f.err
}
func (f fakeProvider) ScrapeLoad(context.Context, domain.ProbeTarget) (domain.LoadObservation, error) {
	return f.load, f.err
}

type fakeElector struct{}

func (fakeElector) Elect(ctx context.Context, callback func(context.Context) error) error {
	return callback(ctx)
}

type fakeReadiness struct {
	mu          sync.Mutex
	values      []bool
	becameReady chan struct{}
	once        sync.Once
}

func (r *fakeReadiness) SetReady(value bool) {
	r.mu.Lock()
	r.values = append(r.values, value)
	r.mu.Unlock()
	if value {
		r.once.Do(func() { close(r.becameReady) })
	}
}

func TestRunLeaderAcquiresGenerationBeforeInitialLevelReconcile(t *testing.T) {
	key, err := domain.NewResourceKey("models", "pod-0")
	if err != nil {
		t.Fatal(err)
	}
	state, err := domain.NewResourceState("cluster-a", key, nil)
	if err != nil {
		t.Fatal(err)
	}
	catalog := &fakeCatalog{}
	readiness := &fakeReadiness{becameReady: make(chan struct{})}
	controller, err := New("cluster-a", "controller-1", 1, 8, catalog, &fakeDiscovery{key: key, state: state}, fakeElector{}, readiness, fakeProvider{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- controller.RunLeader(ctx) }()
	<-readiness.becameReady
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if !reflect.DeepEqual(catalog.events, []string{"acquire"}) {
		t.Fatalf("unexpected operation order: %v", catalog.events)
	}
}

func TestReconcileRecordsExactProbeLoadBeforeCapacity(t *testing.T) {
	key, _ := domain.NewResourceKey("models", "engine")
	identity, _ := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: "cluster", Namespace: "models", LogicalEngine: "engine", PodUID: "pod-uid", EndpointEpoch: 1, RecoveryEpoch: 1})
	projection, _ := domain.NewBackendProjection(domain.BackendProjectionParams{Identity: identity, Model: "model-a", Endpoint: "https://10.0.0.1:9443", ConfiguredSlots: 2, FreshFor: time.Minute})
	state, _ := domain.NewResourceState("cluster", key, []domain.BackendProjection{projection})
	workloadResource, _ := domain.NewResourceRef(domain.ResourceRefParams{Cluster: "cluster", Namespace: "models", Name: "engine", UID: "workload-uid", ResourceVersion: "workload-rv"})
	workload, _ := domain.NewWorkloadRef(domain.WorkloadStatefulSet, workloadResource, 1)
	podResource, _ := domain.NewResourceRef(domain.ResourceRefParams{Cluster: "cluster", Namespace: "models", Name: "engine-0", UID: "pod-uid", ResourceVersion: "pod-rv"})
	pod, _ := domain.NewPodRef(domain.PodRefParams{Resource: podResource, WorkloadUID: workload.UID()})
	fact, _ := domain.NewStructuralFact(domain.StructuralFactParams{Endpoint: projection.Endpoint(), Model: projection.Model(), Workload: workload, Members: []domain.PodRef{pod}, EndpointEpoch: 1, RecoveryEpoch: 1})
	stamp, _ := domain.NewSourceStamp(domain.SourceStructural, 7, 1)
	structural, _ := domain.NewObservation(domain.ObservationParams{Stamp: stamp, Identity: identity, TTLClass: domain.TTLStructural, Structural: fact})
	at := time.Unix(150, 0).UTC()
	health, _ := domain.NewRuntimeHealthObservation(identity, domain.RuntimeHealthResponsive, at)
	load, _ := domain.NewLoadObservation(domain.LoadObservationParams{Identity: identity, ObservedAt: at, UsedSlots: 1, HasUsedSlots: true, QueueDepth: 0, HasQueueDepth: true})
	catalog := &fakeCatalog{}
	source := &fakeDiscovery{key: key, state: state, observations: []domain.Observation{structural}}
	controller, err := New("cluster", "holder", 1, 4, catalog, source, fakeElector{}, &fakeReadiness{becameReady: make(chan struct{})}, fakeProvider{health: health, load: load})
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Reconcile(context.Background(), 7, key); err != nil {
		t.Fatal(err)
	}
	if len(catalog.observations) != 3 || len(catalog.updates) != 1 {
		t.Fatalf("recorded observations=%d capacity updates=%d", len(catalog.observations), len(catalog.updates))
	}
	if catalog.observations[0].Stamp().Kind() != domain.SourceStructural || catalog.observations[1].Stamp().Kind() != domain.SourceRuntimeHealth || catalog.observations[2].Stamp().Kind() != domain.SourceLoad {
		t.Fatal("structural, exact health, and exact load observations were not recorded in order")
	}
	if catalog.updates[0].AdmissionLimit() != 2 {
		t.Fatal("capacity was not synchronized from configured slots")
	}

	controller.provider = fakeProvider{err: errors.New("manifest mismatch")}
	if err := controller.Reconcile(context.Background(), 7, key); err == nil {
		t.Fatal("provider manifest/probe failure did not fail closed")
	}
}
