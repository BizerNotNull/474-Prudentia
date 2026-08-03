package functional_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	controllerapp "github.com/BizerNotNull/474-Prudentia/internal/controller"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

var errAtomicPrecondition = errors.New("kubernetes atomic precondition failed")

type barrierCatalog struct {
	mu              sync.Mutex
	op              domain.WorkloadOperation
	control         *barrierControl
	events          []string
	admissionClosed bool
	reopened        bool
	proofRecorded   bool
	victimsRecorded bool
	recoveryProof   domain.FleetRebuildProof
}

func (c *barrierCatalog) event(value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, value)
}
func (*barrierCatalog) AcquireControllerWriterGeneration(context.Context, string, string) (domain.WriterGeneration, error) {
	return domain.NewWriterGeneration(1)
}
func (*barrierCatalog) ReplaceResourceProjection(context.Context, domain.WriterGeneration, domain.ResourceState) error {
	return nil
}
func (c *barrierCatalog) ListIncompleteWorkloadOperations(context.Context, domain.WriterGeneration) ([]domain.WorkloadOperation, error) {
	return []domain.WorkloadOperation{c.op}, nil
}
func (c *barrierCatalog) AdvanceWorkloadOperationFence(context.Context, domain.WriterGeneration, domain.WorkloadRef, domain.WorkloadOperationIntent) (domain.WorkloadOperation, error) {
	c.event("close-admission-and-advance")
	c.admissionClosed = true
	return c.op, nil
}
func (c *barrierCatalog) RecordWorkloadBarrier(context.Context, domain.WriterGeneration, domain.WorkloadBarrierProof) error {
	c.event("record-barrier")
	c.proofRecorded = true
	return nil
}
func (c *barrierCatalog) WaitForOldCallsQuiescent(context.Context, domain.WriterGeneration, domain.WorkloadOperationRef) error {
	c.event("wait-old-call")
	close(c.control.releaseOld)
	if err := <-c.control.oldResult; !errors.Is(err, errAtomicPrecondition) {
		return errors.New("old mutation did not fail its precondition")
	}
	return nil
}
func (c *barrierCatalog) RecordWorkloadVictims(context.Context, domain.WriterGeneration, domain.WorkloadVictimObservation) error {
	c.event("record-victims")
	c.victimsRecorded = true
	return nil
}
func (c *barrierCatalog) CompleteWorkloadOperationAndReopen(context.Context, domain.WriterGeneration, domain.WorkloadOperationRef) error {
	c.event("reopen")
	if !c.admissionClosed || !c.proofRecorded || !c.victimsRecorded {
		return errors.New("incomplete handoff proof")
	}
	c.reopened = true
	return nil
}
func (c *barrierCatalog) BeginFleetRecovery(context.Context, domain.WriterGeneration, domain.RecoveryEpoch) error {
	c.event("recovery-fence")
	c.admissionClosed = true
	c.reopened = false
	return nil
}
func (c *barrierCatalog) CompleteFleetRecovery(_ context.Context, _ domain.WriterGeneration, proof domain.FleetRebuildProof) error {
	c.event("recovery-reopen")
	if proof.CurrentPodUIDs().Len() == 0 || proof.ProjectionVersions()[0] == 0 {
		return errors.New("incomplete fleet proof")
	}
	c.recoveryProof = proof
	c.reopened = true
	return nil
}

type barrierControl struct {
	workload      domain.WorkloadRef
	pod           domain.PodRef
	proof         domain.WorkloadBarrierProof
	victims       domain.WorkloadVictimObservation
	mu            sync.Mutex
	currentToken  string
	releaseOld    chan struct{}
	oldResult     chan error
	recoveryProof domain.FleetRebuildProof
	rollSeen      bool
	observeErr    error
}

func (*barrierControl) Run(context.Context, func(domain.ResourceKey)) error    { return nil }
func (*barrierControl) WaitForSync(context.Context) error                      { return nil }
func (*barrierControl) ListKeys(context.Context) ([]domain.ResourceKey, error) { return nil, nil }
func (*barrierControl) Reconcile(context.Context, domain.ResourceKey) (domain.ResourceState, error) {
	return domain.ResourceState{}, nil
}
func (c *barrierControl) InstallWorkloadOperationBarrier(context.Context, domain.WorkloadOperation, domain.WorkloadRef, []domain.PodRef) (domain.WorkloadBarrierProof, error) {
	c.mu.Lock()
	c.currentToken = c.proof.Operation().Token()
	c.mu.Unlock()
	return c.proof, nil
}
func (c *barrierControl) ObserveWorkloadVictims(context.Context, domain.WorkloadOperationRef, domain.PodUIDSet) (domain.WorkloadVictimObservation, error) {
	return c.victims, nil
}
func (c *barrierControl) RollManagedFleet(context.Context, domain.RecoveryEpoch) error {
	c.rollSeen = true
	return nil
}
func (c *barrierControl) ObserveFleetRebuild(context.Context, domain.WriterGeneration, domain.RecoveryEpoch) (domain.FleetRebuildProof, error) {
	if c.observeErr != nil {
		return domain.FleetRebuildProof{}, c.observeErr
	}
	if !c.rollSeen {
		return domain.FleetRebuildProof{}, errors.New("fleet was not rolled")
	}
	return c.recoveryProof, nil
}
func (c *barrierControl) delayedOldMutation(expectedToken string) {
	<-c.releaseOld
	c.mu.Lock()
	current := c.currentToken
	c.mu.Unlock()
	if current != expectedToken {
		c.oldResult <- errAtomicPrecondition
		return
	}
	c.oldResult <- nil
}

type noopElector struct{}

func (noopElector) Elect(ctx context.Context, run func(context.Context) error) error { return run(ctx) }

type noopReadiness struct{}

func (noopReadiness) SetReady(bool) {}

func controllerFixture(t *testing.T) (*controllerapp.Controller, *barrierCatalog, *barrierControl, domain.WriterGeneration, domain.WorkloadRef) {
	t.Helper()
	resource, err := domain.NewResourceRef(domain.ResourceRefParams{Cluster: "cluster", Namespace: "models", Name: "engine", UID: "workload-uid", ResourceVersion: "10"})
	if err != nil {
		t.Fatal(err)
	}
	workload, err := domain.NewWorkloadRef(domain.WorkloadDeployment, resource, 1)
	if err != nil {
		t.Fatal(err)
	}
	op, err := domain.NewWorkloadOperation(domain.WorkloadOperationParams{Scope: workload, Intent: domain.OperationHandoff, Phase: domain.OperationBarrierPending, Generation: 2, Token: "new-token", OldCallsQuiescentAfter: time.Unix(100, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	podResource, _ := domain.NewResourceRef(domain.ResourceRefParams{Cluster: "cluster", Namespace: "models", Name: "engine-pod", UID: "new-pod-uid", ResourceVersion: "11"})
	pod, _ := domain.NewPodRef(domain.PodRefParams{Resource: podResource, WorkloadUID: workload.UID(), OperationGeneration: 2, OperationToken: "new-token"})
	proof, _ := domain.NewWorkloadBarrierProof(domain.WorkloadBarrierProofParams{Operation: op.Ref(), Workload: workload, Pods: []domain.PodRef{pod}, ObservedAt: time.Unix(101, 0).UTC()})
	before, _ := domain.NewPodUIDSet([]string{"new-pod-uid"})
	empty, _ := domain.NewPodUIDSet(nil)
	victims, _ := domain.NewWorkloadVictimObservation(domain.WorkloadVictimObservationParams{Operation: op.Ref(), Workload: workload, Before: before, Terminating: empty, Disappeared: empty, Surviving: before, ObservedAt: time.Unix(102, 0).UTC()})
	control := &barrierControl{workload: workload, pod: pod, proof: proof, victims: victims, currentToken: "old-token", releaseOld: make(chan struct{}), oldResult: make(chan error, 1)}
	catalog := &barrierCatalog{op: op, control: control}
	controller, err := controllerapp.New("cluster", "holder", 1, 4, catalog, control, noopElector{}, noopReadiness{})
	if err != nil {
		t.Fatal(err)
	}
	gen, _ := domain.NewWriterGeneration(1)
	return controller, catalog, control, gen, workload
}

func TestControllerHandoffBarrierFencesDelayedOldMutationBeforeReopen(t *testing.T) {
	controller, catalog, control, gen, workload := controllerFixture(t)
	go control.delayedOldMutation("old-token")
	if _, err := controller.FenceWorkloadHandoff(context.Background(), gen, workload); err != nil {
		t.Fatal(err)
	}
	if !catalog.reopened {
		t.Fatal("admission did not reopen after complete barrier proof")
	}
	want := []string{"close-admission-and-advance", "record-barrier", "wait-old-call", "record-victims", "reopen"}
	if len(catalog.events) != len(want) {
		t.Fatalf("events = %v", catalog.events)
	}
	for i := range want {
		if catalog.events[i] != want[i] {
			t.Fatalf("events = %v", catalog.events)
		}
	}
}

func TestRecoveryCannotReopenWithoutObservedFleetProof(t *testing.T) {
	controller, catalog, control, gen, _ := controllerFixture(t)
	epoch, _ := domain.NewRecoveryEpoch(9)
	control.observeErr = errors.New("registry rebuild incomplete")
	if err := controller.RecoverAfterLedgerRestore(context.Background(), gen, epoch); err == nil {
		t.Fatal("recovery reopened without fleet proof")
	}
	if catalog.reopened {
		t.Fatal("admission reopened after failed proof collection")
	}
	oldPods, _ := domain.NewPodUIDSet([]string{"old-pod"})
	currentPods, _ := domain.NewPodUIDSet([]string{"new-pod"})
	proof, err := domain.NewFleetRebuildProof(domain.FleetRebuildProofParams{Epoch: epoch, OldPodUIDs: oldPods, CurrentPodUIDs: currentPods, ProjectionVersions: []domain.ProjectionVersion{1}, ObservedAt: time.Unix(200, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	control.observeErr = nil
	control.recoveryProof = proof
	if err := controller.RecoverAfterLedgerRestore(context.Background(), gen, epoch); err != nil {
		t.Fatal(err)
	}
	if !catalog.reopened || catalog.recoveryProof.Epoch() != epoch {
		t.Fatal("complete fleet proof did not reopen admission")
	}
}
