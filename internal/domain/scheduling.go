package domain

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

var (
	ErrNoCapacity           = errors.New("no schedulable capacity")
	ErrInvalidReference     = errors.New("invalid reservation reference")
	ErrInvalidState         = errors.New("invalid reservation state")
	ErrStaleTarget          = errors.New("dispatch target is stale")
	ErrIdempotencyConflict  = errors.New("idempotency key conflicts with request")
	ErrRequestInProgress    = errors.New("idempotent request is in progress")
	ErrRequestNotReplayable = errors.New("idempotent request is not replayable")
)

const (
	MaxLookupCandidates  = 4
	MaxDigestCandidates  = 4
	MaxCapabilityBytes   = 256
	MaxCatalogCandidates = 4096
)

type LookupPepperVersion uint32
type DigestVersion uint32

type IdempotencyLookupCandidate struct {
	version LookupPepperVersion
	value   [32]byte
}

func NewIdempotencyLookupCandidate(version uint32, value []byte) (IdempotencyLookupCandidate, error) {
	if version == 0 || len(value) != sha256.Size {
		return IdempotencyLookupCandidate{}, fmt.Errorf("invalid idempotency lookup candidate")
	}
	var digest [sha256.Size]byte
	copy(digest[:], value)
	return IdempotencyLookupCandidate{version: LookupPepperVersion(version), value: digest}, nil
}
func (c IdempotencyLookupCandidate) Version() uint32                    { return uint32(c.version) }
func (c IdempotencyLookupCandidate) PepperVersion() LookupPepperVersion { return c.version }
func (c IdempotencyLookupCandidate) Value() [32]byte                    { return c.value }

type RequestDigestCandidate struct {
	version DigestVersion
	value   [32]byte
}
type RequestDigest = RequestDigestCandidate

func NewRequestDigestCandidate(version uint32, value []byte) (RequestDigestCandidate, error) {
	if version == 0 || len(value) != sha256.Size {
		return RequestDigestCandidate{}, fmt.Errorf("invalid request digest candidate")
	}
	var digest [sha256.Size]byte
	copy(digest[:], value)
	return RequestDigestCandidate{version: DigestVersion(version), value: digest}, nil
}
func NewRequestDigest(version DigestVersion, value []byte) (RequestDigest, error) {
	return NewRequestDigestCandidate(uint32(version), value)
}
func (c RequestDigestCandidate) Version() uint32              { return uint32(c.version) }
func (c RequestDigestCandidate) DigestVersion() DigestVersion { return c.version }
func (c RequestDigestCandidate) Value() [32]byte              { return c.value }

type LookupCandidateSet struct {
	candidates   []IdempotencyLookupCandidate
	writeVersion LookupPepperVersion
}

func NewLookupCandidateSet(candidates []IdempotencyLookupCandidate, writeVersion LookupPepperVersion) (LookupCandidateSet, error) {
	if err := validateLookupCandidates(candidates, writeVersion, true); err != nil {
		return LookupCandidateSet{}, err
	}
	return LookupCandidateSet{candidates: append([]IdempotencyLookupCandidate(nil), candidates...), writeVersion: writeVersion}, nil
}
func (s LookupCandidateSet) Candidates() []IdempotencyLookupCandidate {
	return append([]IdempotencyLookupCandidate(nil), s.candidates...)
}
func (s LookupCandidateSet) WriteVersion() LookupPepperVersion { return s.writeVersion }

type DigestSet struct {
	candidates   []RequestDigestCandidate
	writeVersion DigestVersion
}

func NewDigestSet(candidates []RequestDigestCandidate, writeVersion DigestVersion) (DigestSet, error) {
	if err := validateDigestCandidates(candidates, writeVersion); err != nil {
		return DigestSet{}, err
	}
	return DigestSet{candidates: append([]RequestDigestCandidate(nil), candidates...), writeVersion: writeVersion}, nil
}
func (s DigestSet) Candidates() []RequestDigestCandidate {
	return append([]RequestDigestCandidate(nil), s.candidates...)
}
func (s DigestSet) WriteVersion() DigestVersion { return s.writeVersion }

type ScheduleParams struct {
	RequestID             string
	AttemptID             string
	Tenant                string
	IdempotencyCandidates []IdempotencyLookupCandidate
	LookupWriteVersion    uint32
	DigestCandidates      []RequestDigestCandidate
	DigestWriteVersion    uint32
	Model                 string
	SlotCost              uint32
	Features              FeatureSet
	ExecutionBudget       time.Duration
}
type ScheduleCommand struct {
	requestID             RequestID
	attemptID             AttemptID
	tenant                TenantScope
	idempotencyCandidates []IdempotencyLookupCandidate
	lookupWriteVersion    LookupPepperVersion
	digestCandidates      []RequestDigestCandidate
	digestWriteVersion    DigestVersion
	model                 ModelKey
	slotCost              uint32
	features              FeatureSet
	executionBudget       time.Duration
}

func NewScheduleCommand(p ScheduleParams) (ScheduleCommand, error) {
	requestID, err := NewRequestID(p.RequestID)
	if err != nil {
		return ScheduleCommand{}, fmt.Errorf("invalid schedule command")
	}
	attemptID, err := NewAttemptID(p.AttemptID)
	if err != nil {
		return ScheduleCommand{}, fmt.Errorf("invalid schedule command")
	}
	tenant, err := NewTenantScope(p.Tenant)
	if err != nil {
		return ScheduleCommand{}, fmt.Errorf("invalid schedule command")
	}
	model, err := NewModelKey(p.Model)
	if err != nil {
		return ScheduleCommand{}, fmt.Errorf("invalid schedule command")
	}
	features := p.Features
	if p.SlotCost == 0 || p.SlotCost > 1024 || p.ExecutionBudget <= 0 || p.ExecutionBudget > 30*time.Minute || !features.Valid() {
		return ScheduleCommand{}, fmt.Errorf("invalid schedule command")
	}
	if err := validateCandidateSets(p.IdempotencyCandidates, LookupPepperVersion(p.LookupWriteVersion), p.DigestCandidates, DigestVersion(p.DigestWriteVersion)); err != nil {
		return ScheduleCommand{}, err
	}
	return ScheduleCommand{requestID: requestID, attemptID: attemptID, tenant: tenant,
		idempotencyCandidates: append([]IdempotencyLookupCandidate(nil), p.IdempotencyCandidates...), lookupWriteVersion: LookupPepperVersion(p.LookupWriteVersion),
		digestCandidates: append([]RequestDigestCandidate(nil), p.DigestCandidates...), digestWriteVersion: DigestVersion(p.DigestWriteVersion),
		model: model, slotCost: p.SlotCost, features: features, executionBudget: p.ExecutionBudget}, nil
}
func (c ScheduleCommand) RequestID() string              { return c.requestID.value }
func (c ScheduleCommand) RequestIDValue() RequestID      { return c.requestID }
func (c ScheduleCommand) AttemptID() string              { return c.attemptID.value }
func (c ScheduleCommand) AttemptIDValue() AttemptID      { return c.attemptID }
func (c ScheduleCommand) Tenant() string                 { return c.tenant.value }
func (c ScheduleCommand) TenantScope() TenantScope       { return c.tenant }
func (c ScheduleCommand) Model() string                  { return c.model.value }
func (c ScheduleCommand) ModelKey() ModelKey             { return c.model }
func (c ScheduleCommand) SlotCost() uint32               { return c.slotCost }
func (c ScheduleCommand) Features() FeatureSet           { return c.features }
func (c ScheduleCommand) ExecutionBudget() time.Duration { return c.executionBudget }
func (c ScheduleCommand) IdempotencyCandidates() []IdempotencyLookupCandidate {
	return append([]IdempotencyLookupCandidate(nil), c.idempotencyCandidates...)
}
func (c ScheduleCommand) LookupWriteVersion() uint32 { return uint32(c.lookupWriteVersion) }
func (c ScheduleCommand) DigestCandidates() []RequestDigestCandidate {
	return append([]RequestDigestCandidate(nil), c.digestCandidates...)
}
func (c ScheduleCommand) DigestWriteVersion() uint32 { return uint32(c.digestWriteVersion) }
func (c ScheduleCommand) HasIdempotencyKey() bool    { return len(c.idempotencyCandidates) != 0 }
func validateCandidateSets(lookups []IdempotencyLookupCandidate, lookupWrite LookupPepperVersion, digests []RequestDigestCandidate, digestWrite DigestVersion) error {
	if len(lookups) == 0 && lookupWrite != 0 {
		return fmt.Errorf("invalid schedule command")
	}
	if len(lookups) != 0 {
		if err := validateLookupCandidates(lookups, lookupWrite, false); err != nil {
			return err
		}
	}
	return validateDigestCandidates(digests, digestWrite)
}
func validateLookupCandidates(values []IdempotencyLookupCandidate, write LookupPepperVersion, allowEmpty bool) error {
	if len(values) == 0 {
		if allowEmpty && write == 0 {
			return nil
		}
		return fmt.Errorf("invalid candidate set")
	}
	if len(values) > MaxLookupCandidates || write == 0 {
		return fmt.Errorf("invalid candidate set")
	}
	found := false
	for i, value := range values {
		if value.version == 0 || (i > 0 && values[i-1].version >= value.version) {
			return fmt.Errorf("invalid candidate set")
		}
		found = found || value.version == write
	}
	if !found {
		return fmt.Errorf("invalid candidate set")
	}
	return nil
}
func validateDigestCandidates(values []RequestDigestCandidate, write DigestVersion) error {
	if len(values) == 0 || len(values) > MaxDigestCandidates || write == 0 {
		return fmt.Errorf("invalid candidate set")
	}
	found := false
	for i, value := range values {
		if value.version == 0 || (i > 0 && values[i-1].version >= value.version) {
			return fmt.Errorf("invalid candidate set")
		}
		found = found || value.version == write
	}
	if !found {
		return fmt.Errorf("invalid candidate set")
	}
	return nil
}

type ReservationRefParams struct {
	ID         string
	Generation uint64
	Capability []byte
}
type ReservationRef struct {
	id         string
	generation uint64
	capability []byte
}

func NewReservationRef(id string, generation uint64, capability []byte) (ReservationRef, error) {
	return NewReservationRefFromParams(ReservationRefParams{ID: id, Generation: generation, Capability: capability})
}
func NewReservationRefFromParams(p ReservationRefParams) (ReservationRef, error) {
	if !boundedToken(p.ID, 128) || p.Generation == 0 || len(p.Capability) < 16 || len(p.Capability) > MaxCapabilityBytes {
		return ReservationRef{}, ErrInvalidReference
	}
	return ReservationRef{id: p.ID, generation: p.Generation, capability: append([]byte(nil), p.Capability...)}, nil
}
func (r ReservationRef) ID() string         { return r.id }
func (r ReservationRef) Generation() uint64 { return r.generation }
func (r ReservationRef) Capability() []byte { return append([]byte(nil), r.capability...) }
func (r ReservationRef) String() string     { return "reservation[redacted]" }
func (r ReservationRef) GoString() string   { return "ReservationRef{redacted}" }

type Reservation struct{ ref ReservationRef }

func NewReservation(ref ReservationRef) Reservation { return Reservation{ref: ref} }
func (r Reservation) Ref() ReservationRef {
	ref, _ := NewReservationRef(r.ref.id, r.ref.generation, r.ref.capability)
	return ref
}

type WorkloadIdentityParams struct {
	Cluster, Namespace, LogicalEngine, PodUID string
	EndpointEpoch, RecoveryEpoch              uint64
}
type WorkloadIdentity struct {
	cluster, namespace, logicalEngine, podUID string
	endpointEpoch, recoveryEpoch              uint64
}

func NewWorkloadIdentity(p WorkloadIdentityParams) (WorkloadIdentity, error) {
	if !boundedToken(p.Cluster, 128) || !boundedToken(p.Namespace, 128) || !boundedToken(p.LogicalEngine, 256) || !boundedToken(p.PodUID, 128) || p.EndpointEpoch == 0 || p.RecoveryEpoch == 0 {
		return WorkloadIdentity{}, fmt.Errorf("invalid workload identity")
	}
	return WorkloadIdentity{cluster: p.Cluster, namespace: p.Namespace, logicalEngine: p.LogicalEngine, podUID: p.PodUID, endpointEpoch: p.EndpointEpoch, recoveryEpoch: p.RecoveryEpoch}, nil
}
func (i WorkloadIdentity) Cluster() string                   { return i.cluster }
func (i WorkloadIdentity) Namespace() string                 { return i.namespace }
func (i WorkloadIdentity) LogicalEngine() string             { return i.logicalEngine }
func (i WorkloadIdentity) PodUID() string                    { return i.podUID }
func (i WorkloadIdentity) EndpointEpoch() uint64             { return i.endpointEpoch }
func (i WorkloadIdentity) RecoveryEpoch() uint64             { return i.recoveryEpoch }
func (i WorkloadIdentity) Equal(other WorkloadIdentity) bool { return i == other }
func (i WorkloadIdentity) valid() bool {
	return i.cluster != "" && i.namespace != "" && i.logicalEngine != "" && i.podUID != "" && i.endpointEpoch != 0 && i.recoveryEpoch != 0
}
func (i WorkloadIdentity) SPIFFEID(trustDomain string) (*url.URL, error) {
	if !boundedToken(trustDomain, 255) || strings.ContainsAny(trustDomain, "/?#") {
		return nil, fmt.Errorf("invalid trust domain")
	}
	return url.Parse("spiffe://" + trustDomain + "/cluster/" + url.PathEscape(i.cluster) + "/ns/" + url.PathEscape(i.namespace) + "/engine/" + url.PathEscape(i.logicalEngine) + "/pod/" + url.PathEscape(i.podUID) + "/epoch/" + fmt.Sprint(i.endpointEpoch) + "/recovery/" + fmt.Sprint(i.recoveryEpoch))
}

type EndpointRef struct{ value string }

func NewEndpointRef(endpoint string) (EndpointRef, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return EndpointRef{}, fmt.Errorf("invalid endpoint")
	}
	return EndpointRef{value: strings.TrimSuffix(endpoint, "/")}, nil
}
func (r EndpointRef) String() string { return r.value }

type DispatchTarget struct {
	endpoint string
	identity WorkloadIdentity
}

func NewDispatchTarget(endpoint string, identity WorkloadIdentity) (DispatchTarget, error) {
	ref, err := NewEndpointRef(endpoint)
	if err != nil || !identity.valid() {
		return DispatchTarget{}, fmt.Errorf("invalid dispatch target")
	}
	return DispatchTarget{endpoint: ref.value, identity: identity}, nil
}
func (t DispatchTarget) Endpoint() string           { return t.endpoint }
func (t DispatchTarget) Identity() WorkloadIdentity { return t.identity }

type ModelFingerprint struct {
	model    ModelKey
	revision string
}

func NewModelFingerprint(model ModelKey, revision string) (ModelFingerprint, error) {
	if model.value == "" || !boundedToken(revision, 256) {
		return ModelFingerprint{}, fmt.Errorf("invalid model fingerprint")
	}
	return ModelFingerprint{model: model, revision: revision}, nil
}
func (f ModelFingerprint) Model() ModelKey  { return f.model }
func (f ModelFingerprint) Revision() string { return f.revision }

type AdvisoryLoad struct{ utilizationBasisPoints uint16 }

func NewAdvisoryLoad(utilizationBasisPoints uint16) (AdvisoryLoad, error) {
	if utilizationBasisPoints > 10000 {
		return AdvisoryLoad{}, fmt.Errorf("invalid advisory load")
	}
	return AdvisoryLoad{utilizationBasisPoints: utilizationBasisPoints}, nil
}
func (l AdvisoryLoad) UtilizationBasisPoints() uint16 { return l.utilizationBasisPoints }

type SnapshotParams struct {
	Identity                                      WorkloadIdentity
	Endpoint                                      EndpointRef
	Model                                         ModelFingerprint
	Capabilities                                  FeatureSet
	Structural                                    StoredSourceStamp
	Health                                        StoredSourceStamp
	Load                                          StoredSourceStamp
	HasLoadStamp                                  bool
	HealthState                                   HealthState
	DrainState                                    DrainState
	ConfiguredSlots, ReservedSlots, OrphanedSlots uint32
	AdvisoryLoad                                  AdvisoryLoad
	HasAdvisoryLoad                               bool
	CacheHints                                    []CacheHint
	ProjectionVersion                             uint64
	CatalogAsOf                                   time.Time
}
type InstanceSnapshot struct {
	identity                                      WorkloadIdentity
	endpoint                                      EndpointRef
	model                                         ModelFingerprint
	capabilities                                  FeatureSet
	structural, health                            StoredSourceStamp
	load                                          StoredSourceStamp
	hasLoadStamp                                  bool
	healthState                                   HealthState
	drainState                                    DrainState
	configuredSlots, reservedSlots, orphanedSlots uint32
	advisoryLoad                                  AdvisoryLoad
	hasAdvisoryLoad                               bool
	cacheHints                                    []CacheHint
	projectionVersion                             uint64
	catalogAsOf                                   time.Time
}

func NewInstanceSnapshot(p SnapshotParams) (InstanceSnapshot, error) {
	if !p.Identity.valid() || p.Endpoint.value == "" || p.Model.model.value == "" || !p.Capabilities.Valid() || !p.HealthState.valid() || !p.DrainState.valid() || p.ConfiguredSlots == 0 || uint64(p.ReservedSlots)+uint64(p.OrphanedSlots) > uint64(p.ConfiguredSlots) || p.ProjectionVersion == 0 || p.CatalogAsOf.IsZero() || p.CatalogAsOf.After(time.Now()) {
		return InstanceSnapshot{}, fmt.Errorf("invalid instance snapshot")
	}
	if p.Structural.source.kind != SourceStructural || p.Health.source.kind != SourceRuntimeHealth || (p.HasLoadStamp && p.Load.source.kind != SourceLoad) || !p.Structural.validAt(p.Identity, p.CatalogAsOf) || !p.Health.validAt(p.Identity, p.CatalogAsOf) || (p.HasLoadStamp && !p.Load.validAt(p.Identity, p.CatalogAsOf)) || p.HasAdvisoryLoad != p.HasLoadStamp {
		return InstanceSnapshot{}, fmt.Errorf("invalid instance snapshot")
	}
	hints := append([]CacheHint(nil), p.CacheHints...)
	if len(hints) > 64 {
		return InstanceSnapshot{}, fmt.Errorf("invalid instance snapshot")
	}
	for _, h := range hints {
		if h.source != p.Identity || h.expiresAt.IsZero() || (h.kind != CacheCatalog && h.kind != CacheHit) {
			return InstanceSnapshot{}, fmt.Errorf("invalid instance snapshot")
		}
	}
	return InstanceSnapshot{identity: p.Identity, endpoint: p.Endpoint, model: p.Model, capabilities: p.Capabilities, structural: p.Structural, health: p.Health, load: p.Load, hasLoadStamp: p.HasLoadStamp, healthState: p.HealthState, drainState: p.DrainState, configuredSlots: p.ConfiguredSlots, reservedSlots: p.ReservedSlots, orphanedSlots: p.OrphanedSlots, advisoryLoad: p.AdvisoryLoad, hasAdvisoryLoad: p.HasAdvisoryLoad, cacheHints: hints, projectionVersion: p.ProjectionVersion, catalogAsOf: p.CatalogAsOf}, nil
}
func (s InstanceSnapshot) Identity() WorkloadIdentity           { return s.identity }
func (s InstanceSnapshot) Endpoint() EndpointRef                { return s.endpoint }
func (s InstanceSnapshot) Model() ModelFingerprint              { return s.model }
func (s InstanceSnapshot) Capabilities() FeatureSet             { return s.capabilities }
func (s InstanceSnapshot) StructuralStamp() StoredSourceStamp   { return s.structural }
func (s InstanceSnapshot) HealthStamp() StoredSourceStamp       { return s.health }
func (s InstanceSnapshot) LoadStamp() (StoredSourceStamp, bool) { return s.load, s.hasLoadStamp }
func (s InstanceSnapshot) HealthState() HealthState             { return s.healthState }
func (s InstanceSnapshot) DrainState() DrainState               { return s.drainState }
func (s InstanceSnapshot) ConfiguredSlots() uint32              { return s.configuredSlots }
func (s InstanceSnapshot) ReservedSlots() uint32                { return s.reservedSlots }
func (s InstanceSnapshot) OrphanedSlots() uint32                { return s.orphanedSlots }
func (s InstanceSnapshot) AvailableSlots() uint32 {
	return s.configuredSlots - s.reservedSlots - s.orphanedSlots
}
func (s InstanceSnapshot) AdvisoryLoad() (AdvisoryLoad, bool) {
	return s.advisoryLoad, s.hasAdvisoryLoad
}
func (s InstanceSnapshot) CacheHints() []CacheHint   { return append([]CacheHint(nil), s.cacheHints...) }
func (s InstanceSnapshot) ProjectionVersion() uint64 { return s.projectionVersion }
func (s InstanceSnapshot) CatalogAsOf() time.Time    { return s.catalogAsOf }

type CandidateCatalog struct {
	candidates []InstanceSnapshot
	asOf       time.Time
}

func NewCandidateCatalog(candidates []InstanceSnapshot, asOf time.Time) (CandidateCatalog, error) {
	if asOf.IsZero() || asOf.After(time.Now()) || len(candidates) > MaxCatalogCandidates {
		return CandidateCatalog{}, fmt.Errorf("invalid candidate catalog")
	}
	cloned := append([]InstanceSnapshot(nil), candidates...)
	seen := make(map[WorkloadIdentity]struct{}, len(cloned))
	for i := range cloned {
		if cloned[i].catalogAsOf != asOf {
			return CandidateCatalog{}, fmt.Errorf("invalid candidate catalog")
		}
		if _, ok := seen[cloned[i].identity]; ok {
			return CandidateCatalog{}, fmt.Errorf("invalid candidate catalog")
		}
		seen[cloned[i].identity] = struct{}{}
		cloned[i].cacheHints = append([]CacheHint(nil), cloned[i].cacheHints...)
	}
	sort.Slice(cloned, func(i, j int) bool { return identitySortKey(cloned[i].identity) < identitySortKey(cloned[j].identity) })
	return CandidateCatalog{candidates: cloned, asOf: asOf}, nil
}
func (c CandidateCatalog) Candidates() []InstanceSnapshot {
	result := append([]InstanceSnapshot(nil), c.candidates...)
	for i := range result {
		result[i].cacheHints = append([]CacheHint(nil), result[i].cacheHints...)
	}
	return result
}
func (c CandidateCatalog) AsOf() time.Time { return c.asOf }
func identitySortKey(i WorkloadIdentity) string {
	return i.cluster + "\x00" + i.namespace + "\x00" + i.logicalEngine + "\x00" + i.podUID + fmt.Sprintf("\x00%020d\x00%020d", i.endpointEpoch, i.recoveryEpoch)
}

type PlacementPolicyParams struct {
	Version        uint16
	RequiredHealth HealthState
	MaxSnapshotAge time.Duration
	PreferCache    bool
}
type PlacementPolicy struct {
	version        uint16
	requiredHealth HealthState
	maxSnapshotAge time.Duration
	preferCache    bool
}

func NewPlacementPolicy(p PlacementPolicyParams) (PlacementPolicy, error) {
	if p.Version != 1 || !p.RequiredHealth.valid() || p.MaxSnapshotAge <= 0 || p.MaxSnapshotAge > time.Hour {
		return PlacementPolicy{}, fmt.Errorf("invalid placement policy")
	}
	return PlacementPolicy{version: p.Version, requiredHealth: p.RequiredHealth, maxSnapshotAge: p.MaxSnapshotAge, preferCache: p.PreferCache}, nil
}
func (p PlacementPolicy) Version() uint16               { return p.version }
func (p PlacementPolicy) RequiredHealth() HealthState   { return p.requiredHealth }
func (p PlacementPolicy) MaxSnapshotAge() time.Duration { return p.maxSnapshotAge }
func (p PlacementPolicy) PreferCache() bool             { return p.preferCache }

type RankedCandidate struct {
	snapshot InstanceSnapshot
	score    int64
	rank     uint32
}

func NewRankedCandidate(snapshot InstanceSnapshot, score int64, rank uint32) (RankedCandidate, error) {
	if !snapshot.identity.valid() || rank == 0 {
		return RankedCandidate{}, fmt.Errorf("invalid ranked candidate")
	}
	snapshot.cacheHints = append([]CacheHint(nil), snapshot.cacheHints...)
	return RankedCandidate{snapshot: snapshot, score: score, rank: rank}, nil
}
func (c RankedCandidate) Snapshot() InstanceSnapshot {
	result := c.snapshot
	result.cacheHints = append([]CacheHint(nil), result.cacheHints...)
	return result
}
func (c RankedCandidate) Score() int64 { return c.score }
func (c RankedCandidate) Rank() uint32 { return c.rank }

type PlacementRejection uint8

const (
	PlacementRejectedModel PlacementRejection = iota + 1
	PlacementRejectedFeatures
	PlacementRejectedHealth
	PlacementRejectedDrain
	PlacementRejectedCapacity
	PlacementRejectedStale
)

type PlacementExplanationParams struct {
	PolicyVersion       uint16
	Evaluated, Eligible uint32
	Rejections          []PlacementRejection
}
type PlacementExplanation struct {
	policyVersion       uint16
	evaluated, eligible uint32
	rejections          []PlacementRejection
}

func NewPlacementExplanation(p PlacementExplanationParams) (PlacementExplanation, error) {
	if p.PolicyVersion != 1 || p.Eligible > p.Evaluated || len(p.Rejections) > int(p.Evaluated) {
		return PlacementExplanation{}, fmt.Errorf("invalid placement explanation")
	}
	for _, r := range p.Rejections {
		if r < PlacementRejectedModel || r > PlacementRejectedStale {
			return PlacementExplanation{}, fmt.Errorf("invalid placement explanation")
		}
	}
	return PlacementExplanation{policyVersion: p.PolicyVersion, evaluated: p.Evaluated, eligible: p.Eligible, rejections: append([]PlacementRejection(nil), p.Rejections...)}, nil
}
func (e PlacementExplanation) PolicyVersion() uint16 { return e.policyVersion }
func (e PlacementExplanation) Evaluated() uint32     { return e.evaluated }
func (e PlacementExplanation) Eligible() uint32      { return e.eligible }
func (e PlacementExplanation) Rejections() []PlacementRejection {
	return append([]PlacementRejection(nil), e.rejections...)
}

type AmbiguousCause uint8

const (
	AmbiguousTransport AmbiguousCause = iota + 1
	AmbiguousCanceled
	AmbiguousProtocol
)

type RerankReason uint8

const RerankStaleTarget RerankReason = 1

type GiveUpReason uint8

const (
	GiveUpCanceled GiveUpReason = iota + 1
	GiveUpBudgetExpired
	GiveUpReranksExhausted
)

func CapabilityHash(capability []byte) [32]byte { return sha256.Sum256(capability) }
func CapabilityMatches(capability, expectedHash []byte) bool {
	actual := CapabilityHash(capability)
	return len(expectedHash) == len(actual) && subtle.ConstantTimeCompare(actual[:], expectedHash) == 1
}
func boundedToken(value string, max int) bool { return boundedValue(value, max) }
