package domain

import (
	"strings"
	"testing"
	"time"
)

func operationFixture(t *testing.T, token string) (WorkloadRef, PodRef, WorkloadOperationRef) {
	t.Helper()
	wr, err := NewResourceRef(ResourceRefParams{Cluster: "c", Namespace: "n", Name: "engine", UID: "workload-uid", ResourceVersion: "20"})
	if err != nil { t.Fatal(err) }
	workload, err := NewWorkloadRef(WorkloadStatefulSet, wr, 1)
	if err != nil { t.Fatal(err) }
	pr, err := NewResourceRef(ResourceRefParams{Cluster: "c", Namespace: "n", Name: "engine-0", UID: "pod-uid", ResourceVersion: "21"})
	if err != nil { t.Fatal(err) }
	pod, err := NewPodRef(PodRefParams{Resource: pr, WorkloadUID: "workload-uid", OperationGeneration: 8, OperationToken: token})
	if err != nil { t.Fatal(err) }
	op, err := NewWorkloadOperationRef("workload-uid", 8, token)
	if err != nil { t.Fatal(err) }
	return workload, pod, op
}

func TestOperationTokenBoundsAndPhaseValidation(t *testing.T) {
	workload, _, _ := operationFixture(t, "token")
	if _, err := NewWorkloadOperationRef("workload-uid", 1, strings.Repeat("x", maxOperationTokenBytes+1)); err == nil { t.Fatal("accepted oversized token") }
	if _, err := NewWorkloadOperationRef("workload-uid", 1, " token"); err == nil { t.Fatal("accepted noncanonical token") }
	if _, err := NewWorkloadOperation(WorkloadOperationParams{Scope: workload, Intent: OperationDrain, Phase: 0, Generation: 1, Token: "token", OldCallsQuiescentAfter: time.Now()}); err == nil { t.Fatal("accepted unknown operation phase") }
	if _, err := NewWorkloadOperation(WorkloadOperationParams{Scope: workload, Intent: 255, Phase: OperationBarrierPending, Generation: 1, Token: "token", OldCallsQuiescentAfter: time.Now()}); err == nil { t.Fatal("accepted unknown operation intent") }
	ref, _ := NewWorkloadOperationRef("workload-uid", 1, "secret-looking-token")
	if strings.Contains(ref.String(), "secret-looking-token") { t.Fatal("operation String exposed token") }
}

func TestBarrierAndRemovalBindExactOperation(t *testing.T) {
	workload, pod, op := operationFixture(t, "token")
	at := time.Unix(200, 0).UTC()
	input := []PodRef{pod}
	proof, err := NewWorkloadBarrierProof(WorkloadBarrierProofParams{Operation: op, Workload: workload, Pods: input, ObservedAt: at})
	if err != nil { t.Fatal(err) }
	input[0] = PodRef{}
	if proof.Pods()[0].UID() != "pod-uid" { t.Fatal("proof did not copy Pod refs") }
	returned := proof.Pods(); returned[0] = PodRef{}
	if proof.Pods()[0].UID() != "pod-uid" { t.Fatal("proof accessor leaked slice") }

	wrongToken, _ := NewWorkloadOperationRef("workload-uid", 8, "other")
	if _, err := NewWorkloadBarrierProof(WorkloadBarrierProofParams{Operation: wrongToken, Workload: workload, Pods: []PodRef{pod}, ObservedAt: at}); err == nil { t.Fatal("accepted Pod proof for wrong token") }
	if _, err := NewRemovalResult(wrongToken, pod, RemovalDelete); err == nil { t.Fatal("accepted removal for wrong token") }
	if _, err := NewRemovalResult(op, pod, 0); err == nil { t.Fatal("accepted unknown removal mode") }
}

func TestVictimAndCompletionProofsAreComplete(t *testing.T) {
	workload, pod, op := operationFixture(t, "token")
	at := time.Unix(300, 0).UTC()
	barrier, err := NewWorkloadBarrierProof(WorkloadBarrierProofParams{Operation: op, Workload: workload, Pods: []PodRef{pod}, ObservedAt: at})
	if err != nil { t.Fatal(err) }
	before, _ := NewPodUIDSet([]string{"pod-uid"})
	empty, _ := NewPodUIDSet(nil)
	surviving, _ := NewPodUIDSet([]string{"pod-uid"})
	victims, err := NewWorkloadVictimObservation(WorkloadVictimObservationParams{Operation: op, Workload: workload, Before: before, Terminating: empty, Disappeared: empty, Surviving: surviving, ObservedAt: at.Add(time.Second)})
	if err != nil { t.Fatal(err) }
	if _, err := NewWorkloadVictimObservation(WorkloadVictimObservationParams{Operation: op, Workload: workload, Before: before, Terminating: empty, Disappeared: empty, Surviving: empty, ObservedAt: at}); err == nil { t.Fatal("accepted incomplete victim partition") }
	completion, err := NewWorkloadCompletionProof(WorkloadCompletionProofParams{Barrier: barrier, Victims: victims, Current: workload, CurrentPods: []PodRef{pod}, DesiredReplicas: 1, CompletedAt: at.Add(2*time.Second)})
	if err != nil { t.Fatal(err) }
	pods := completion.CurrentPods(); pods[0] = PodRef{}
	if completion.CurrentPods()[0].UID() != "pod-uid" { t.Fatal("completion proof leaked Pod slice") }
}

func TestUIDSetsUsageAndRecoveryAreImmutable(t *testing.T) {
	values := []string{"b", "a"}
	set, err := NewPodUIDSet(values)
	if err != nil { t.Fatal(err) }
	values[0] = "changed"
	if got := set.Values(); len(got) != 2 || got[0] != "a" || got[1] != "b" { t.Fatalf("set not canonical and copied: %v", got) }
	got := set.Values(); got[0] = "changed"
	if !set.Contains("a") { t.Fatal("set accessor mutated value") }
	if _, err := NewPodUIDSet([]string{"a", "a"}); err == nil { t.Fatal("accepted duplicate UID") }

	current, _ := NewPodUIDSet([]string{"new-pod"})
	old, _ := NewPodUIDSet([]string{"old-pod"})
	versions := []ProjectionVersion{3}
	proof, err := NewFleetRebuildProof(FleetRebuildProofParams{Epoch: 5, OldPodUIDs: old, CurrentPodUIDs: current, ProjectionVersions: versions, ObservedAt: time.Now()})
	if err != nil { t.Fatal(err) }
	versions[0] = 9
	returned := proof.ProjectionVersions(); returned[0] = 9
	if proof.ProjectionVersions()[0] != 3 { t.Fatal("fleet proof leaked projection versions") }
	overlap, _ := NewPodUIDSet([]string{"old-pod"})
	if _, err := NewFleetRebuildProof(FleetRebuildProofParams{Epoch: 5, OldPodUIDs: old, CurrentPodUIDs: overlap, ProjectionVersions: []ProjectionVersion{1}, ObservedAt: time.Now()}); err == nil { t.Fatal("accepted old Pod in rebuilt fleet") }
}

func TestDrainDesiredAndApplyModes(t *testing.T) {
	workload, _, _ := operationFixture(t, "token")
	scope, _ := NewWorkloadDrainScope(workload)
	if _, err := NewDrainIntent(DrainIntentParams{Scope: scope, State: 0, Reason: "maintenance"}); err == nil { t.Fatal("accepted unknown drain state") }
	intent, err := NewDrainIntent(DrainIntentParams{Scope: scope, State: DrainActive, Reason: "maintenance"})
	if err != nil { t.Fatal(err) }
	if _, ok := intent.Deadline(); ok { t.Fatal("absent deadline reported present") }
	if _, err := NewApplyResult(0, workload); err == nil { t.Fatal("accepted unknown apply mode") }

	key, _ := NewResourceKey("n", "engine")
	if _, err := NewDesiredModel(DesiredModelParams{Key: key, Model: "model", Managed: false, ReplicaAuthority: ReplicaAuthorityPrudentia, Generation: 1, RecoveryEpoch: 1}); err == nil { t.Fatal("accepted replica ownership for unmanaged model") }
}
