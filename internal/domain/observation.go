package domain

import (
	"fmt"
	"math"
	"time"
)

// SourceKind identifies the authority that produced a normalized fact.
type SourceKind uint8

const (
	SourceStructural SourceKind = iota + 1
	SourceRuntimeHealth
	SourceLoad
)

func (k SourceKind) Valid() bool { return k >= SourceStructural && k <= SourceLoad }

type SourceSequence uint64

func NewSourceSequence(value uint64) (SourceSequence, error) {
	if value == 0 {
		return 0, fmt.Errorf("invalid source sequence")
	}
	return SourceSequence(value), nil
}
func (s SourceSequence) Uint64() uint64 { return uint64(s) }

type TTLClass uint8

const (
	TTLStructural TTLClass = iota + 1
	TTLRuntimeHealth
	TTLLoad
)

func (c TTLClass) Valid() bool { return c >= TTLStructural && c <= TTLLoad }

// SourceStamp is supplied by an observer. Database acceptance time is deliberately absent.
type SourceStamp struct {
	kind       SourceKind
	generation WriterGeneration
	sequence   SourceSequence
}

func NewSourceStamp(kind SourceKind, generation WriterGeneration, sequence SourceSequence) (SourceStamp, error) {
	if !kind.Valid() || generation == 0 || sequence == 0 {
		return SourceStamp{}, fmt.Errorf("invalid source stamp")
	}
	return SourceStamp{kind: kind, generation: generation, sequence: sequence}, nil
}
func (s SourceStamp) Kind() SourceKind                   { return s.kind }
func (s SourceStamp) WriterGeneration() WriterGeneration { return s.generation }
func (s SourceStamp) Sequence() SourceSequence           { return s.sequence }

// StoredSourceStamp is the catalog-assigned freshness stamp. Its times are database times.
type StoredSourceStamp struct {
	source     SourceStamp
	acceptedAt time.Time
	expiresAt  time.Time
}

func NewStoredSourceStamp(source SourceStamp, acceptedAt, expiresAt time.Time) (StoredSourceStamp, error) {
	if source.kind == 0 || acceptedAt.IsZero() || !expiresAt.After(acceptedAt) {
		return StoredSourceStamp{}, fmt.Errorf("invalid stored source stamp")
	}
	return StoredSourceStamp{source: source, acceptedAt: acceptedAt, expiresAt: expiresAt}, nil
}
func (s StoredSourceStamp) Source() SourceStamp   { return s.source }
func (s StoredSourceStamp) AcceptedAt() time.Time { return s.acceptedAt }
func (s StoredSourceStamp) ExpiresAt() time.Time  { return s.expiresAt }
func (s StoredSourceStamp) FreshAt(at time.Time) bool {
	return !at.Before(s.acceptedAt) && at.Before(s.expiresAt)
}

type HealthState uint8

const (
	HealthStarting HealthState = iota + 1
	HealthReady
	HealthDegraded
	HealthUnhealthy
)

func (s HealthState) Valid() bool { return s >= HealthStarting && s <= HealthUnhealthy }

type StructuralFactParams struct {
	Endpoint      string
	Model         string
	Workload      WorkloadRef
	Members       []PodRef
	EndpointEpoch uint64
	RecoveryEpoch uint64
}

type StructuralFact struct {
	endpoint      string
	model         string
	workload      WorkloadRef
	members       []PodRef
	endpointEpoch uint64
	recoveryEpoch uint64
}

func NewStructuralFact(p StructuralFactParams) (StructuralFact, error) {
	if !boundedToken(p.Model, 256) || p.EndpointEpoch == 0 || p.RecoveryEpoch == 0 || !p.Workload.valid() || len(p.Members) == 0 || len(p.Members) > 1024 {
		return StructuralFact{}, fmt.Errorf("invalid structural fact")
	}
	targetIdentity, err := NewWorkloadIdentity(WorkloadIdentityParams{Cluster: p.Workload.cluster, Namespace: p.Workload.namespace, LogicalEngine: p.Model, PodUID: string(p.Members[0].uid), EndpointEpoch: p.EndpointEpoch, RecoveryEpoch: p.RecoveryEpoch})
	if err != nil {
		return StructuralFact{}, err
	}
	if _, err = NewDispatchTarget(p.Endpoint, targetIdentity); err != nil {
		return StructuralFact{}, err
	}
	members := make([]PodRef, len(p.Members))
	seen := make(map[string]struct{}, len(members))
	for i, member := range p.Members {
		if !member.valid() || member.cluster != p.Workload.cluster || member.namespace != p.Workload.namespace || member.workloadUID != string(p.Workload.uid) {
			return StructuralFact{}, fmt.Errorf("invalid structural membership")
		}
		if _, ok := seen[string(member.uid)]; ok {
			return StructuralFact{}, fmt.Errorf("duplicate structural member")
		}
		seen[string(member.uid)] = struct{}{}
		members[i] = member
	}
	return StructuralFact{endpoint: p.Endpoint, model: p.Model, workload: p.Workload, members: members, endpointEpoch: p.EndpointEpoch, recoveryEpoch: p.RecoveryEpoch}, nil
}
func (f StructuralFact) Endpoint() string      { return f.endpoint }
func (f StructuralFact) Model() string         { return f.model }
func (f StructuralFact) Workload() WorkloadRef { return f.workload }
func (f StructuralFact) Members() []PodRef     { return append([]PodRef(nil), f.members...) }
func (f StructuralFact) EndpointEpoch() uint64 { return f.endpointEpoch }
func (f StructuralFact) RecoveryEpoch() uint64 { return f.recoveryEpoch }

type RuntimeHealthFact struct {
	state HealthState
	warm  bool
}

func NewRuntimeHealthFact(state HealthState, warm bool) (RuntimeHealthFact, error) {
	if !state.Valid() || (warm && state != HealthReady && state != HealthDegraded) {
		return RuntimeHealthFact{}, fmt.Errorf("invalid runtime health fact")
	}
	return RuntimeHealthFact{state: state, warm: warm}, nil
}
func (f RuntimeHealthFact) State() HealthState { return f.state }
func (f RuntimeHealthFact) Warm() bool         { return f.warm }

type LoadFactParams struct {
	RunningRequests uint32
	QueuedRequests  uint32
	Utilization     float64
	HasUtilization  bool
}
type LoadFact struct {
	running, queued uint32
	utilization     float64
	hasUtilization  bool
	observed        bool
}

func NewLoadFact(p LoadFactParams) (LoadFact, error) {
	if p.RunningRequests > 1_000_000 || p.QueuedRequests > 1_000_000 || math.IsNaN(p.Utilization) || math.IsInf(p.Utilization, 0) || (p.HasUtilization && (p.Utilization < 0 || p.Utilization > 1)) || (!p.HasUtilization && p.Utilization != 0) {
		return LoadFact{}, fmt.Errorf("invalid load fact")
	}
	return LoadFact{running: p.RunningRequests, queued: p.QueuedRequests, utilization: p.Utilization, hasUtilization: p.HasUtilization, observed: true}, nil
}
func (f LoadFact) RunningRequests() uint32      { return f.running }
func (f LoadFact) QueuedRequests() uint32       { return f.queued }
func (f LoadFact) Utilization() (float64, bool) { return f.utilization, f.hasUtilization }

type ObservationParams struct {
	Stamp               SourceStamp
	Identity            WorkloadIdentity
	TTLClass            TTLClass
	Structural          StructuralFact
	RuntimeHealth       RuntimeHealthFact
	Load                LoadFact
	SourceReportedAt    time.Time
	HasSourceReportedAt bool
}
type Observation struct {
	stamp               SourceStamp
	identity            WorkloadIdentity
	ttlClass            TTLClass
	structural          StructuralFact
	health              RuntimeHealthFact
	load                LoadFact
	sourceReportedAt    time.Time
	hasSourceReportedAt bool
}

func NewObservation(p ObservationParams) (Observation, error) {
	if !p.Stamp.kind.Valid() || !p.TTLClass.Valid() || p.Identity.PodUID() == "" || (!p.HasSourceReportedAt && !p.SourceReportedAt.IsZero()) || (p.HasSourceReportedAt && p.SourceReportedAt.IsZero()) {
		return Observation{}, fmt.Errorf("invalid observation")
	}
	switch p.Stamp.kind {
	case SourceStructural:
		if p.TTLClass != TTLStructural || len(p.Structural.members) == 0 || p.RuntimeHealth.state != 0 || p.Load.observed {
			return Observation{}, fmt.Errorf("invalid structural observation")
		}
		found := false
		for _, member := range p.Structural.members {
			found = found || string(member.uid) == p.Identity.PodUID()
		}
		if !found || p.Structural.endpointEpoch != p.Identity.EndpointEpoch() || p.Structural.recoveryEpoch != p.Identity.RecoveryEpoch() {
			return Observation{}, fmt.Errorf("structural observation identity mismatch")
		}
	case SourceRuntimeHealth:
		if p.TTLClass != TTLRuntimeHealth || !p.RuntimeHealth.state.Valid() || len(p.Structural.members) != 0 || p.Load.observed {
			return Observation{}, fmt.Errorf("invalid health observation")
		}
	case SourceLoad:
		if p.TTLClass != TTLLoad || !p.Load.observed || len(p.Structural.members) != 0 || p.RuntimeHealth.state != 0 {
			return Observation{}, fmt.Errorf("invalid load observation")
		}
	}
	return Observation{stamp: p.Stamp, identity: p.Identity, ttlClass: p.TTLClass, structural: p.Structural, health: p.RuntimeHealth, load: p.Load, sourceReportedAt: p.SourceReportedAt, hasSourceReportedAt: p.HasSourceReportedAt}, nil
}
func (o Observation) Stamp() SourceStamp         { return o.stamp }
func (o Observation) Identity() WorkloadIdentity { return o.identity }
func (o Observation) TTLClass() TTLClass         { return o.ttlClass }
func (o Observation) Structural() (StructuralFact, bool) {
	return o.structural, o.stamp.kind == SourceStructural
}
func (o Observation) RuntimeHealth() (RuntimeHealthFact, bool) {
	return o.health, o.stamp.kind == SourceRuntimeHealth
}
func (o Observation) Load() (LoadFact, bool) { return o.load, o.stamp.kind == SourceLoad }
func (o Observation) SourceReportedAt() (time.Time, bool) {
	return o.sourceReportedAt, o.hasSourceReportedAt
}

type ProjectionVersion uint64

func NewProjectionVersion(value uint64) (ProjectionVersion, error) {
	if value == 0 {
		return 0, fmt.Errorf("invalid projection version")
	}
	return ProjectionVersion(value), nil
}
func (v ProjectionVersion) Uint64() uint64 { return uint64(v) }

type ProjectionUpdateParams struct {
	Identity        WorkloadIdentity
	Structural      StoredSourceStamp
	Health          StoredSourceStamp
	Load            StoredSourceStamp
	HasLoad         bool
	ConfiguredSlots uint32
	AdmissionLimit  uint32
	PreviousVersion ProjectionVersion
}
type ProjectionUpdate struct {
	identity                        WorkloadIdentity
	structural, health, load        StoredSourceStamp
	hasLoad                         bool
	configuredSlots, admissionLimit uint32
	previousVersion                 ProjectionVersion
}

func NewProjectionUpdate(p ProjectionUpdateParams) (ProjectionUpdate, error) {
	if p.Identity.PodUID() == "" || p.Structural.source.kind != SourceStructural || p.Health.source.kind != SourceRuntimeHealth || (p.HasLoad && p.Load.source.kind != SourceLoad) || (!p.HasLoad && p.Load.source.kind != 0) || p.ConfiguredSlots == 0 || p.ConfiguredSlots > 1024 || p.AdmissionLimit > p.ConfiguredSlots {
		return ProjectionUpdate{}, fmt.Errorf("invalid projection update")
	}
	return ProjectionUpdate{identity: p.Identity, structural: p.Structural, health: p.Health, load: p.Load, hasLoad: p.HasLoad, configuredSlots: p.ConfiguredSlots, admissionLimit: p.AdmissionLimit, previousVersion: p.PreviousVersion}, nil
}
func (p ProjectionUpdate) Identity() WorkloadIdentity           { return p.identity }
func (p ProjectionUpdate) StructuralStamp() StoredSourceStamp   { return p.structural }
func (p ProjectionUpdate) HealthStamp() StoredSourceStamp       { return p.health }
func (p ProjectionUpdate) LoadStamp() (StoredSourceStamp, bool) { return p.load, p.hasLoad }
func (p ProjectionUpdate) ConfiguredSlots() uint32              { return p.configuredSlots }
func (p ProjectionUpdate) AdmissionLimit() uint32               { return p.admissionLimit }
func (p ProjectionUpdate) PreviousVersion() ProjectionVersion   { return p.previousVersion }
