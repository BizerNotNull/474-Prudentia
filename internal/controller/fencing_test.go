package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type fencingCatalog struct {
	fakeCatalog
	op          domain.WorkloadOperation
	barrierSeen chan struct{}
	allowOld    chan struct{}
	completed   chan struct{}
	once        sync.Once
}

func (f *fencingCatalog) ListIncompleteWorkloadOperations(context.Context, domain.WriterGeneration) ([]domain.WorkloadOperation, error) {
	return []domain.WorkloadOperation{f.op}, nil
}
func (f *fencingCatalog) AdvanceWorkloadOperationFence(context.Context, domain.WriterGeneration, domain.WorkloadRef, domain.WorkloadOperationIntent) (domain.WorkloadOperation, error) {
	f.record("advance")
	return f.op, nil
}
func (f *fencingCatalog) RecordWorkloadBarrier(context.Context, domain.WriterGeneration, domain.WorkloadBarrierProof) error {
	f.record("barrier")
	f.once.Do(func() { close(f.barrierSeen) })
	return nil
}
func (f *fencingCatalog) WaitForOldCallsQuiescent(ctx context.Context, _ domain.WriterGeneration, _ domain.WorkloadOperationRef) error {
	f.record("wait")
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.allowOld:
		return nil
	}
}
func (f *fencingCatalog) RecordWorkloadVictims(context.Context, domain.WriterGeneration, domain.WorkloadVictimObservation) error {
	f.record("victims")
	return nil
}
func (f *fencingCatalog) CompleteWorkloadOperationAndReopen(context.Context, domain.WriterGeneration, domain.WorkloadCompletionProof) error {
	f.record("complete")
	close(f.completed)
	return nil
}
func (f *fencingCatalog) record(event string) {
	f.mu.Lock()
	f.events = append(f.events, event)
	f.mu.Unlock()
}

type fencingSource struct {
	fakeDiscovery
	proof      domain.WorkloadBarrierProof
	victims    domain.WorkloadVictimObservation
	completion domain.WorkloadCompletionProof
	reconciled chan struct{}
	once       sync.Once
}

func (f *fencingSource) Reconcile(context.Context, domain.ResourceKey) (domain.ResourceState, error) {
	f.once.Do(func() { close(f.reconciled) })
	return f.state, nil
}
func (f *fencingSource) InstallWorkloadOperationBarrier(context.Context, domain.WorkloadOperation, domain.WorkloadRef, []domain.PodRef) (domain.WorkloadBarrierProof, error) {
	return f.proof, nil
}
func (f *fencingSource) ObserveWorkloadVictims(context.Context, domain.WorkloadOperationRef, domain.PodUIDSet) (domain.WorkloadVictimObservation, error) {
	return f.victims, nil
}
func (f *fencingSource) BuildWorkloadCompletionProof(context.Context, domain.WorkloadBarrierProof, domain.WorkloadVictimObservation) (domain.WorkloadCompletionProof, error) {
	return f.completion, nil
}

func TestRunLeaderDoesNotReopenOrWriteBeforeHandoffQuiescence(t *testing.T) {
	key, _ := domain.NewResourceKey("models", "svc")
	resource, _ := domain.NewResourceRef(domain.ResourceRefParams{Cluster: "cluster", Namespace: "models", Name: "engine", UID: "workload-uid", ResourceVersion: "10"})
	workload, _ := domain.NewWorkloadRef(domain.WorkloadDeployment, resource, 1)
	op, _ := domain.NewWorkloadOperation(domain.WorkloadOperationParams{Scope: workload, Intent: domain.OperationHandoff, Phase: domain.OperationBarrierPending, Generation: 2, Token: "new-token", OldCallsQuiescentAfter: time.Unix(100, 0).UTC()})
	podResource, _ := domain.NewResourceRef(domain.ResourceRefParams{Cluster: "cluster", Namespace: "models", Name: "engine-pod", UID: "pod-uid", ResourceVersion: "11"})
	pod, _ := domain.NewPodRef(domain.PodRefParams{Resource: podResource, WorkloadUID: workload.UID(), OperationGeneration: 2, OperationToken: "new-token"})
	proof, _ := domain.NewWorkloadBarrierProof(domain.WorkloadBarrierProofParams{Operation: op.Ref(), Workload: workload, Pods: []domain.PodRef{pod}, ObservedAt: time.Unix(101, 0).UTC()})
	before, _ := domain.NewPodUIDSet([]string{"pod-uid"})
	empty, _ := domain.NewPodUIDSet(nil)
	victims, _ := domain.NewWorkloadVictimObservation(domain.WorkloadVictimObservationParams{Operation: op.Ref(), Workload: workload, Before: before, Terminating: empty, Disappeared: empty, Surviving: before, ObservedAt: time.Unix(102, 0).UTC()})
	completion, _ := domain.NewWorkloadCompletionProof(domain.WorkloadCompletionProofParams{Barrier: proof, Victims: victims, Current: workload, CurrentPods: []domain.PodRef{pod}, DesiredReplicas: 1, CompletedAt: time.Unix(103, 0).UTC()})
	state, _ := domain.NewResourceState("cluster", key, nil)
	catalog := &fencingCatalog{op: op, barrierSeen: make(chan struct{}), allowOld: make(chan struct{}), completed: make(chan struct{})}
	source := &fencingSource{fakeDiscovery: fakeDiscovery{key: key, state: state}, proof: proof, victims: victims, completion: completion, reconciled: make(chan struct{})}
	ready := &fakeReadiness{becameReady: make(chan struct{})}
	controller, err := New("cluster", "holder", 1, 4, catalog, source, fakeElector{}, ready, fakeProvider{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- controller.RunLeader(ctx) }()
	<-catalog.barrierSeen
	catalog.mu.Lock()
	statesBefore := len(catalog.states)
	catalog.mu.Unlock()
	if statesBefore != 0 {
		t.Fatal("ordinary reconcile wrote before handoff fence completed")
	}
	select {
	case <-catalog.completed:
		t.Fatal("admission reopened before old calls quiesced")
	default:
	}
	close(catalog.allowOld)
	<-catalog.completed
	<-source.reconciled
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
