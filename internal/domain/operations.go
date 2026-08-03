package domain

import (
	"fmt"
	"slices"
	"sort"
	"time"
)

const maxOperationTokenBytes = 256

type ResourceUID string
type ResourceVersion string

func validResourceUID(v ResourceUID) bool         { return boundedToken(string(v), 128) }
func validResourceVersion(v ResourceVersion) bool { return boundedToken(string(v), 256) }

type ResourceRefParams struct{ Cluster, Namespace, Name, UID, ResourceVersion string }
type ResourceRef struct {
	cluster, namespace, name string
	uid                      ResourceUID
	resourceVersion          ResourceVersion
}

func NewResourceRef(p ResourceRefParams) (ResourceRef, error) {
	r := ResourceRef{cluster: p.Cluster, namespace: p.Namespace, name: p.Name, uid: ResourceUID(p.UID), resourceVersion: ResourceVersion(p.ResourceVersion)}
	if !r.valid() {
		return ResourceRef{}, fmt.Errorf("invalid resource reference")
	}
	return r, nil
}
func (r ResourceRef) valid() bool {
	return boundedToken(r.cluster, 128) && boundedToken(r.namespace, 253) && boundedToken(r.name, 253) && validResourceUID(r.uid) && validResourceVersion(r.resourceVersion)
}
func (r ResourceRef) Cluster() string         { return r.cluster }
func (r ResourceRef) Namespace() string       { return r.namespace }
func (r ResourceRef) Name() string            { return r.name }
func (r ResourceRef) UID() string             { return string(r.uid) }
func (r ResourceRef) ResourceVersion() string { return string(r.resourceVersion) }

type WorkloadKind uint8

const (
	WorkloadDeployment WorkloadKind = iota + 1
	WorkloadStatefulSet
)

func (k WorkloadKind) Valid() bool { return k == WorkloadDeployment || k == WorkloadStatefulSet }

type WorkloadRef struct {
	ResourceRef
	kind     WorkloadKind
	replicas int32
}

func NewWorkloadRef(kind WorkloadKind, resource ResourceRef, replicas int32) (WorkloadRef, error) {
	if !kind.Valid() || !resource.valid() || replicas < 0 || replicas > 100_000 {
		return WorkloadRef{}, fmt.Errorf("invalid workload reference")
	}
	return WorkloadRef{ResourceRef: resource, kind: kind, replicas: replicas}, nil
}
func (r WorkloadRef) valid() bool {
	return r.kind.Valid() && r.ResourceRef.valid() && r.replicas >= 0 && r.replicas <= 100_000
}
func (r WorkloadRef) Kind() WorkloadKind { return r.kind }
func (r WorkloadRef) Replicas() int32    { return r.replicas }

type DeploymentRef struct{ workload WorkloadRef }

func NewDeploymentRef(r WorkloadRef) (DeploymentRef, error) {
	if r.kind != WorkloadDeployment {
		return DeploymentRef{}, fmt.Errorf("not a deployment")
	}
	return DeploymentRef{r}, nil
}
func (r DeploymentRef) Workload() WorkloadRef { return r.workload }

type StatefulSetRef struct{ workload WorkloadRef }

func NewStatefulSetRef(r WorkloadRef) (StatefulSetRef, error) {
	if r.kind != WorkloadStatefulSet {
		return StatefulSetRef{}, fmt.Errorf("not a stateful set")
	}
	return StatefulSetRef{r}, nil
}
func (r StatefulSetRef) Workload() WorkloadRef { return r.workload }

type PodRefParams struct {
	Resource            ResourceRef
	WorkloadUID         string
	OperationGeneration uint64
	OperationToken      string
}
type PodRef struct {
	ResourceRef
	workloadUID         string
	operationGeneration uint64
	operationToken      string
}

func NewPodRef(p PodRefParams) (PodRef, error) {
	r := PodRef{ResourceRef: p.Resource, workloadUID: p.WorkloadUID, operationGeneration: p.OperationGeneration, operationToken: p.OperationToken}
	if !r.ResourceRef.valid() || !boundedToken(r.workloadUID, 128) || (r.operationGeneration == 0) != (r.operationToken == "") || (r.operationToken != "" && !boundedToken(r.operationToken, maxOperationTokenBytes)) {
		return PodRef{}, fmt.Errorf("invalid Pod reference")
	}
	return r, nil
}
func (r PodRef) valid() bool {
	return r.ResourceRef.valid() && boundedToken(r.workloadUID, 128) && (r.operationGeneration == 0) == (r.operationToken == "") && (r.operationToken == "" || boundedToken(r.operationToken, maxOperationTokenBytes))
}
func (r PodRef) WorkloadUID() string            { return r.workloadUID }
func (r PodRef) OperationGeneration() uint64    { return r.operationGeneration }
func (r PodRef) OperationToken() (string, bool) { return r.operationToken, r.operationToken != "" }

type PodUIDSet struct{ values []string }

func NewPodUIDSet(values []string) (PodUIDSet, error) {
	if len(values) > 100_000 {
		return PodUIDSet{}, fmt.Errorf("too many Pod UIDs")
	}
	copyValues := append([]string(nil), values...)
	for _, v := range copyValues {
		if !boundedToken(v, 128) {
			return PodUIDSet{}, fmt.Errorf("invalid Pod UID")
		}
	}
	sort.Strings(copyValues)
	for i := 1; i < len(copyValues); i++ {
		if copyValues[i] == copyValues[i-1] {
			return PodUIDSet{}, fmt.Errorf("duplicate Pod UID")
		}
	}
	return PodUIDSet{values: copyValues}, nil
}
func (s PodUIDSet) Values() []string         { return append([]string(nil), s.values...) }
func (s PodUIDSet) Len() int                 { return len(s.values) }
func (s PodUIDSet) Contains(uid string) bool { _, ok := slices.BinarySearch(s.values, uid); return ok }

type ReplicaAuthority uint8

const (
	ReplicaAuthorityPrudentia ReplicaAuthority = iota + 1
	ReplicaAuthorityExternal
)

func (a ReplicaAuthority) Valid() bool {
	return a == ReplicaAuthorityPrudentia || a == ReplicaAuthorityExternal
}

type DesiredModelParams struct {
	Key              ResourceKey
	Model            string
	Managed          bool
	ReplicaAuthority ReplicaAuthority
	Replicas         int32
	Generation       uint64
	RecoveryEpoch    uint64
}
type DesiredModel struct {
	key                       ResourceKey
	model                     string
	managed                   bool
	replicaAuthority          ReplicaAuthority
	replicas                  int32
	generation, recoveryEpoch uint64
}

func NewDesiredModel(p DesiredModelParams) (DesiredModel, error) {
	if !boundedToken(p.Key.namespace, 253) || !boundedToken(p.Key.name, 253) || !boundedToken(p.Model, 256) || !p.ReplicaAuthority.Valid() || p.Replicas < 0 || p.Replicas > 100_000 || p.Generation == 0 || p.RecoveryEpoch == 0 {
		return DesiredModel{}, fmt.Errorf("invalid desired model")
	}
	if !p.Managed && p.ReplicaAuthority == ReplicaAuthorityPrudentia {
		return DesiredModel{}, fmt.Errorf("unmanaged model cannot own replicas")
	}
	return DesiredModel{key: p.Key, model: p.Model, managed: p.Managed, replicaAuthority: p.ReplicaAuthority, replicas: p.Replicas, generation: p.Generation, recoveryEpoch: p.RecoveryEpoch}, nil
}
func (d DesiredModel) Key() ResourceKey                   { return d.key }
func (d DesiredModel) Model() string                      { return d.model }
func (d DesiredModel) Managed() bool                      { return d.managed }
func (d DesiredModel) ReplicaAuthority() ReplicaAuthority { return d.replicaAuthority }
func (d DesiredModel) Replicas() int32                    { return d.replicas }
func (d DesiredModel) Generation() uint64                 { return d.generation }
func (d DesiredModel) RecoveryEpoch() uint64              { return d.recoveryEpoch }

type ApplyMode uint8

const (
	ApplyUnchanged ApplyMode = iota + 1
	ApplyCreated
	ApplyUpdated
	ApplyConflict
)

func (m ApplyMode) Valid() bool { return m >= ApplyUnchanged && m <= ApplyConflict }

type ApplyResult struct {
	mode ApplyMode
	ref  WorkloadRef
}

func NewApplyResult(mode ApplyMode, ref WorkloadRef) (ApplyResult, error) {
	if !mode.Valid() || !ref.valid() {
		return ApplyResult{}, fmt.Errorf("invalid apply result")
	}
	return ApplyResult{mode, ref}, nil
}
func (r ApplyResult) Mode() ApplyMode       { return r.mode }
func (r ApplyResult) Workload() WorkloadRef { return r.ref }

type DrainState uint8

const (
	DrainReady DrainState = iota + 1
	DrainRequested
	DrainActive
	DrainForced
	DrainRemoving
	DrainComplete
)

// Snapshot aliases preserve the scheduling vocabulary without defining a
// second drain-state type.
const (
	DrainStateReady     = DrainReady
	DrainStateRequested = DrainRequested
	DrainStateActive    = DrainActive
	DrainStateForced    = DrainForced
	DrainStateRemoving  = DrainRemoving
	DrainStateComplete  = DrainComplete
)

func (s DrainState) Valid() bool { return s >= DrainReady && s <= DrainComplete }
func (s DrainState) valid() bool { return s.Valid() }

type DrainScopeKind uint8

const (
	DrainInstance DrainScopeKind = iota + 1
	DrainWorkload
)

type DrainScope struct {
	kind     DrainScopeKind
	identity WorkloadIdentity
	workload WorkloadRef
}

func NewInstanceDrainScope(id WorkloadIdentity) (DrainScope, error) {
	if id.PodUID() == "" {
		return DrainScope{}, fmt.Errorf("invalid drain identity")
	}
	return DrainScope{kind: DrainInstance, identity: id}, nil
}
func NewWorkloadDrainScope(ref WorkloadRef) (DrainScope, error) {
	if !ref.valid() {
		return DrainScope{}, fmt.Errorf("invalid drain workload")
	}
	return DrainScope{kind: DrainWorkload, workload: ref}, nil
}
func (s DrainScope) Kind() DrainScopeKind               { return s.kind }
func (s DrainScope) Identity() (WorkloadIdentity, bool) { return s.identity, s.kind == DrainInstance }
func (s DrainScope) Workload() (WorkloadRef, bool)      { return s.workload, s.kind == DrainWorkload }

type DrainIntentParams struct {
	Scope       DrainScope
	State       DrainState
	Reason      string
	Deadline    time.Time
	HasDeadline bool
}
type DrainIntent struct {
	scope       DrainScope
	state       DrainState
	reason      string
	deadline    time.Time
	hasDeadline bool
}

func NewDrainIntent(p DrainIntentParams) (DrainIntent, error) {
	if (p.Scope.kind != DrainInstance && p.Scope.kind != DrainWorkload) || !p.State.Valid() || !boundedToken(p.Reason, 512) || (!p.HasDeadline && !p.Deadline.IsZero()) || (p.HasDeadline && p.Deadline.IsZero()) {
		return DrainIntent{}, fmt.Errorf("invalid drain intent")
	}
	return DrainIntent{p.Scope, p.State, p.Reason, p.Deadline, p.HasDeadline}, nil
}
func (d DrainIntent) Scope() DrainScope           { return d.scope }
func (d DrainIntent) State() DrainState           { return d.state }
func (d DrainIntent) Reason() string              { return d.reason }
func (d DrainIntent) Deadline() (time.Time, bool) { return d.deadline, d.hasDeadline }

type DrainCommand struct{ intent DrainIntent }

func NewDrainCommand(intent DrainIntent) (DrainCommand, error) {
	if !intent.state.Valid() {
		return DrainCommand{}, fmt.Errorf("invalid drain command")
	}
	return DrainCommand{intent}, nil
}
func (c DrainCommand) Intent() DrainIntent { return c.intent }

type UsageByPodParams struct {
	PodUID                      string
	Reservations, OrphanedSlots uint32
}
type UsageByPod struct {
	podUID                      string
	reservations, orphanedSlots uint32
}

func (u UsageByPod) PodUID() string        { return u.podUID }
func (u UsageByPod) Reservations() uint32  { return u.reservations }
func (u UsageByPod) OrphanedSlots() uint32 { return u.orphanedSlots }

type ActiveUsage struct {
	scope DrainScope
	pods  []UsageByPod
}

func NewActiveUsage(scope DrainScope, values []UsageByPodParams) (ActiveUsage, error) {
	if scope.kind != DrainInstance && scope.kind != DrainWorkload {
		return ActiveUsage{}, fmt.Errorf("invalid usage scope")
	}
	if len(values) > 100_000 {
		return ActiveUsage{}, fmt.Errorf("too many usage entries")
	}
	pods := make([]UsageByPod, len(values))
	seen := map[string]struct{}{}
	for i, v := range values {
		if !boundedToken(v.PodUID, 128) {
			return ActiveUsage{}, fmt.Errorf("invalid usage Pod")
		}
		if _, ok := seen[v.PodUID]; ok {
			return ActiveUsage{}, fmt.Errorf("duplicate usage Pod")
		}
		seen[v.PodUID] = struct{}{}
		pods[i] = UsageByPod{v.PodUID, v.Reservations, v.OrphanedSlots}
	}
	return ActiveUsage{scope, pods}, nil
}
func (u ActiveUsage) Scope() DrainScope  { return u.scope }
func (u ActiveUsage) Pods() []UsageByPod { return append([]UsageByPod(nil), u.pods...) }

type WorkloadOperationIntent uint8

const (
	OperationDrain WorkloadOperationIntent = iota + 1
	OperationScaleDown
	OperationExactRemoval
	OperationRecovery
	OperationHandoff
)

func (i WorkloadOperationIntent) Valid() bool { return i >= OperationDrain && i <= OperationHandoff }

type WorkloadOperationPhase uint8

const (
	OperationBarrierPending WorkloadOperationPhase = iota + 1
	OperationBarrierObserved
	OperationMutating
	OperationObservingVictims
	OperationComplete
)

func (p WorkloadOperationPhase) Valid() bool {
	return p >= OperationBarrierPending && p <= OperationComplete
}

type WorkloadOperationRef struct {
	workloadUID string
	generation  uint64
	token       string
}

func NewWorkloadOperationRef(workloadUID string, generation uint64, token string) (WorkloadOperationRef, error) {
	if !boundedToken(workloadUID, 128) || generation == 0 || !boundedToken(token, maxOperationTokenBytes) {
		return WorkloadOperationRef{}, fmt.Errorf("invalid workload operation reference")
	}
	return WorkloadOperationRef{workloadUID, generation, token}, nil
}
func (r WorkloadOperationRef) WorkloadUID() string { return r.workloadUID }
func (r WorkloadOperationRef) Generation() uint64  { return r.generation }
func (r WorkloadOperationRef) Token() string       { return r.token }
func (r WorkloadOperationRef) String() string      { return "workload-operation[redacted]" }

type WorkloadOperationParams struct {
	Scope                  WorkloadRef
	Intent                 WorkloadOperationIntent
	Phase                  WorkloadOperationPhase
	Generation             uint64
	Token                  string
	OldCallsQuiescentAfter time.Time
}
type WorkloadOperation struct {
	scope                  WorkloadRef
	intent                 WorkloadOperationIntent
	phase                  WorkloadOperationPhase
	ref                    WorkloadOperationRef
	oldCallsQuiescentAfter time.Time
}

func NewWorkloadOperation(p WorkloadOperationParams) (WorkloadOperation, error) {
	if !p.Scope.valid() || !p.Intent.Valid() || !p.Phase.Valid() || p.OldCallsQuiescentAfter.IsZero() {
		return WorkloadOperation{}, fmt.Errorf("invalid workload operation")
	}
	ref, e := NewWorkloadOperationRef(string(p.Scope.uid), p.Generation, p.Token)
	if e != nil {
		return WorkloadOperation{}, e
	}
	return WorkloadOperation{p.Scope, p.Intent, p.Phase, ref, p.OldCallsQuiescentAfter}, nil
}
func (o WorkloadOperation) Scope() WorkloadRef                { return o.scope }
func (o WorkloadOperation) Intent() WorkloadOperationIntent   { return o.intent }
func (o WorkloadOperation) Phase() WorkloadOperationPhase     { return o.phase }
func (o WorkloadOperation) Ref() WorkloadOperationRef         { return o.ref }
func (o WorkloadOperation) OldCallsQuiescentAfter() time.Time { return o.oldCallsQuiescentAfter }

type WorkloadBarrierProofParams struct {
	Operation  WorkloadOperationRef
	Workload   WorkloadRef
	Pods       []PodRef
	ObservedAt time.Time
}
type WorkloadBarrierProof struct {
	operation  WorkloadOperationRef
	workload   WorkloadRef
	pods       []PodRef
	observedAt time.Time
}

func NewWorkloadBarrierProof(p WorkloadBarrierProofParams) (WorkloadBarrierProof, error) {
	if p.Operation.workloadUID == "" || !p.Workload.valid() || p.Workload.uid != ResourceUID(p.Operation.workloadUID) || p.ObservedAt.IsZero() || len(p.Pods) > 100_000 {
		return WorkloadBarrierProof{}, fmt.Errorf("invalid workload barrier proof")
	}
	pods := append([]PodRef(nil), p.Pods...)
	seen := map[string]struct{}{}
	for _, pod := range pods {
		token, ok := pod.OperationToken()
		if !pod.valid() || pod.workloadUID != p.Operation.workloadUID || !ok || token != p.Operation.token || pod.operationGeneration != p.Operation.generation {
			return WorkloadBarrierProof{}, fmt.Errorf("Pod barrier does not bind operation")
		}
		if _, ok := seen[string(pod.uid)]; ok {
			return WorkloadBarrierProof{}, fmt.Errorf("duplicate barrier Pod")
		}
		seen[string(pod.uid)] = struct{}{}
	}
	return WorkloadBarrierProof{p.Operation, p.Workload, pods, p.ObservedAt}, nil
}
func (p WorkloadBarrierProof) Operation() WorkloadOperationRef { return p.operation }
func (p WorkloadBarrierProof) Workload() WorkloadRef           { return p.workload }
func (p WorkloadBarrierProof) Pods() []PodRef                  { return append([]PodRef(nil), p.pods...) }
func (p WorkloadBarrierProof) ObservedAt() time.Time           { return p.observedAt }

type WorkloadVictimObservationParams struct {
	Operation                                   WorkloadOperationRef
	Workload                                    WorkloadRef
	Before, Terminating, Disappeared, Surviving PodUIDSet
	ObservedAt                                  time.Time
}
type WorkloadVictimObservation struct {
	operation                                   WorkloadOperationRef
	workload                                    WorkloadRef
	before, terminating, disappeared, surviving PodUIDSet
	observedAt                                  time.Time
}

func NewWorkloadVictimObservation(p WorkloadVictimObservationParams) (WorkloadVictimObservation, error) {
	if p.Operation.workloadUID == "" || !p.Workload.valid() || string(p.Workload.uid) != p.Operation.workloadUID || p.ObservedAt.IsZero() {
		return WorkloadVictimObservation{}, fmt.Errorf("invalid victim observation")
	}
	seen := map[string]struct{}{}
	for _, set := range []PodUIDSet{p.Terminating, p.Disappeared, p.Surviving} {
		for _, uid := range set.values {
			if !p.Before.Contains(uid) {
				return WorkloadVictimObservation{}, fmt.Errorf("victim outside original set")
			}
			if _, ok := seen[uid]; ok {
				return WorkloadVictimObservation{}, fmt.Errorf("overlapping victim sets")
			}
			seen[uid] = struct{}{}
		}
	}
	if len(seen) != p.Before.Len() {
		return WorkloadVictimObservation{}, fmt.Errorf("incomplete victim observation")
	}
	return WorkloadVictimObservation{p.Operation, p.Workload, p.Before, p.Terminating, p.Disappeared, p.Surviving, p.ObservedAt}, nil
}
func (o WorkloadVictimObservation) Operation() WorkloadOperationRef { return o.operation }
func (o WorkloadVictimObservation) Before() PodUIDSet {
	return PodUIDSet{append([]string(nil), o.before.values...)}
}
func (o WorkloadVictimObservation) Terminating() PodUIDSet {
	return PodUIDSet{append([]string(nil), o.terminating.values...)}
}
func (o WorkloadVictimObservation) Disappeared() PodUIDSet {
	return PodUIDSet{append([]string(nil), o.disappeared.values...)}
}
func (o WorkloadVictimObservation) Surviving() PodUIDSet {
	return PodUIDSet{append([]string(nil), o.surviving.values...)}
}

type WorkloadCompletionProofParams struct {
	Barrier         WorkloadBarrierProof
	Victims         WorkloadVictimObservation
	Current         WorkloadRef
	CurrentPods     []PodRef
	DesiredReplicas int32
	CompletedAt     time.Time
}
type WorkloadCompletionProof struct {
	barrier         WorkloadBarrierProof
	victims         WorkloadVictimObservation
	current         WorkloadRef
	currentPods     []PodRef
	desiredReplicas int32
	completedAt     time.Time
}

func NewWorkloadCompletionProof(p WorkloadCompletionProofParams) (WorkloadCompletionProof, error) {
	if p.Barrier.operation != p.Victims.operation || p.Barrier.operation.workloadUID == "" || !p.Current.valid() || string(p.Current.uid) != p.Barrier.operation.workloadUID || p.DesiredReplicas < 0 || int32(len(p.CurrentPods)) != p.DesiredReplicas || p.Current.replicas != p.DesiredReplicas || p.CompletedAt.IsZero() || p.CompletedAt.Before(p.Barrier.observedAt) {
		return WorkloadCompletionProof{}, fmt.Errorf("invalid workload completion proof")
	}
	pods := append([]PodRef(nil), p.CurrentPods...)
	for _, pod := range pods {
		if !pod.valid() || pod.workloadUID != p.Barrier.operation.workloadUID {
			return WorkloadCompletionProof{}, fmt.Errorf("invalid completion Pod")
		}
	}
	return WorkloadCompletionProof{p.Barrier, p.Victims, p.Current, pods, p.DesiredReplicas, p.CompletedAt}, nil
}
func (p WorkloadCompletionProof) Barrier() WorkloadBarrierProof { return p.barrier }
func (p WorkloadCompletionProof) CurrentPods() []PodRef {
	return append([]PodRef(nil), p.currentPods...)
}
func (p WorkloadCompletionProof) CompletedAt() time.Time { return p.completedAt }

type RemovalMode uint8

const (
	RemovalDelete RemovalMode = iota + 1
	RemovalEvict
)

func (m RemovalMode) Valid() bool { return m == RemovalDelete || m == RemovalEvict }

type RemovalResult struct {
	operation WorkloadOperationRef
	pod       PodRef
	mode      RemovalMode
}

func NewRemovalResult(op WorkloadOperationRef, pod PodRef, mode RemovalMode) (RemovalResult, error) {
	token, ok := pod.OperationToken()
	if !mode.Valid() || !ok || token != op.token || pod.operationGeneration != op.generation || pod.workloadUID != op.workloadUID {
		return RemovalResult{}, fmt.Errorf("invalid removal result")
	}
	return RemovalResult{op, pod, mode}, nil
}
func (r RemovalResult) Operation() WorkloadOperationRef { return r.operation }
func (r RemovalResult) Pod() PodRef                     { return r.pod }
func (r RemovalResult) Mode() RemovalMode               { return r.mode }

type ScalePatchResult struct {
	operation                     WorkloadOperationRef
	workload                      WorkloadRef
	previousReplicas, newReplicas int32
}

func NewScalePatchResult(op WorkloadOperationRef, workload WorkloadRef, previousReplicas, newReplicas int32) (ScalePatchResult, error) {
	if string(workload.uid) != op.workloadUID || previousReplicas < 0 || newReplicas < 0 || newReplicas >= previousReplicas || workload.replicas != newReplicas {
		return ScalePatchResult{}, fmt.Errorf("invalid scale patch result")
	}
	return ScalePatchResult{op, workload, previousReplicas, newReplicas}, nil
}
func (r ScalePatchResult) Operation() WorkloadOperationRef { return r.operation }
func (r ScalePatchResult) Workload() WorkloadRef           { return r.workload }
func (r ScalePatchResult) PreviousReplicas() int32         { return r.previousReplicas }
func (r ScalePatchResult) NewReplicas() int32              { return r.newReplicas }

type RecoveryEpoch uint64

func NewRecoveryEpoch(value uint64) (RecoveryEpoch, error) {
	if value == 0 {
		return 0, fmt.Errorf("invalid recovery epoch")
	}
	return RecoveryEpoch(value), nil
}
func (e RecoveryEpoch) Uint64() uint64 { return uint64(e) }

type FleetRebuildProofParams struct {
	Epoch                      RecoveryEpoch
	OldPodUIDs, CurrentPodUIDs PodUIDSet
	ProjectionVersions         []ProjectionVersion
	ObservedAt                 time.Time
}
type FleetRebuildProof struct {
	epoch        RecoveryEpoch
	old, current PodUIDSet
	versions     []ProjectionVersion
	observedAt   time.Time
}

func NewFleetRebuildProof(p FleetRebuildProofParams) (FleetRebuildProof, error) {
	if p.Epoch == 0 || p.ObservedAt.IsZero() || p.CurrentPodUIDs.Len() == 0 || len(p.ProjectionVersions) != p.CurrentPodUIDs.Len() {
		return FleetRebuildProof{}, fmt.Errorf("invalid fleet rebuild proof")
	}
	for _, uid := range p.CurrentPodUIDs.values {
		if p.OldPodUIDs.Contains(uid) {
			return FleetRebuildProof{}, fmt.Errorf("old Pod remains in rebuilt fleet")
		}
	}
	versions := append([]ProjectionVersion(nil), p.ProjectionVersions...)
	for _, v := range versions {
		if v == 0 {
			return FleetRebuildProof{}, fmt.Errorf("invalid fleet projection version")
		}
	}
	return FleetRebuildProof{p.Epoch, PodUIDSet{append([]string(nil), p.OldPodUIDs.values...)}, PodUIDSet{append([]string(nil), p.CurrentPodUIDs.values...)}, versions, p.ObservedAt}, nil
}
func (p FleetRebuildProof) Epoch() RecoveryEpoch { return p.epoch }
func (p FleetRebuildProof) OldPodUIDs() PodUIDSet {
	return PodUIDSet{append([]string(nil), p.old.values...)}
}
func (p FleetRebuildProof) CurrentPodUIDs() PodUIDSet {
	return PodUIDSet{append([]string(nil), p.current.values...)}
}
func (p FleetRebuildProof) ProjectionVersions() []ProjectionVersion {
	return append([]ProjectionVersion(nil), p.versions...)
}
func (p FleetRebuildProof) ObservedAt() time.Time { return p.observedAt }
