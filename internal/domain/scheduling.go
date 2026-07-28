package domain

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/url"
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
	MaxLookupCandidates = 4
	MaxDigestCandidates = 4
)

type IdempotencyLookupCandidate struct {
	version uint32
	value   [32]byte
}

func NewIdempotencyLookupCandidate(version uint32, value []byte) (IdempotencyLookupCandidate, error) {
	if version == 0 || len(value) != sha256.Size {
		return IdempotencyLookupCandidate{}, fmt.Errorf("invalid idempotency lookup candidate")
	}
	var digest [sha256.Size]byte
	copy(digest[:], value)
	return IdempotencyLookupCandidate{version: version, value: digest}, nil
}

func (c IdempotencyLookupCandidate) Version() uint32 { return c.version }
func (c IdempotencyLookupCandidate) Value() [32]byte { return c.value }

type RequestDigestCandidate struct {
	version uint32
	value   [32]byte
}

func NewRequestDigestCandidate(version uint32, value []byte) (RequestDigestCandidate, error) {
	if version == 0 || len(value) != sha256.Size {
		return RequestDigestCandidate{}, fmt.Errorf("invalid request digest candidate")
	}
	var digest [sha256.Size]byte
	copy(digest[:], value)
	return RequestDigestCandidate{version: version, value: digest}, nil
}

func (c RequestDigestCandidate) Version() uint32 { return c.version }
func (c RequestDigestCandidate) Value() [32]byte { return c.value }

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
	ExecutionBudget       time.Duration
}

type ScheduleCommand struct {
	requestID             string
	attemptID             string
	tenant                string
	idempotencyCandidates []IdempotencyLookupCandidate
	lookupWriteVersion    uint32
	digestCandidates      []RequestDigestCandidate
	digestWriteVersion    uint32
	model                 string
	slotCost              uint32
	executionBudget       time.Duration
}

func NewScheduleCommand(p ScheduleParams) (ScheduleCommand, error) {
	if !boundedToken(p.RequestID, 128) || !boundedToken(p.AttemptID, 128) || !boundedToken(p.Tenant, 128) || !boundedToken(p.Model, 256) {
		return ScheduleCommand{}, fmt.Errorf("invalid schedule command")
	}
	if p.SlotCost == 0 || p.SlotCost > 1024 || p.ExecutionBudget <= 0 || p.ExecutionBudget > 30*time.Minute {
		return ScheduleCommand{}, fmt.Errorf("invalid schedule command")
	}
	if err := validateCandidateSets(p.IdempotencyCandidates, p.LookupWriteVersion, p.DigestCandidates, p.DigestWriteVersion); err != nil {
		return ScheduleCommand{}, err
	}
	return ScheduleCommand{
		requestID: p.RequestID, attemptID: p.AttemptID, tenant: p.Tenant,
		idempotencyCandidates: append([]IdempotencyLookupCandidate(nil), p.IdempotencyCandidates...),
		lookupWriteVersion:    p.LookupWriteVersion,
		digestCandidates:      append([]RequestDigestCandidate(nil), p.DigestCandidates...),
		digestWriteVersion:    p.DigestWriteVersion,
		model:                 p.Model, slotCost: p.SlotCost, executionBudget: p.ExecutionBudget,
	}, nil
}

func (c ScheduleCommand) RequestID() string { return c.requestID }
func (c ScheduleCommand) AttemptID() string { return c.attemptID }
func (c ScheduleCommand) Tenant() string    { return c.tenant }
func (c ScheduleCommand) Model() string     { return c.model }
func (c ScheduleCommand) SlotCost() uint32  { return c.slotCost }
func (c ScheduleCommand) ExecutionBudget() time.Duration {
	return c.executionBudget
}
func (c ScheduleCommand) IdempotencyCandidates() []IdempotencyLookupCandidate {
	return append([]IdempotencyLookupCandidate(nil), c.idempotencyCandidates...)
}
func (c ScheduleCommand) LookupWriteVersion() uint32 { return c.lookupWriteVersion }
func (c ScheduleCommand) DigestCandidates() []RequestDigestCandidate {
	return append([]RequestDigestCandidate(nil), c.digestCandidates...)
}
func (c ScheduleCommand) DigestWriteVersion() uint32 { return c.digestWriteVersion }
func (c ScheduleCommand) HasIdempotencyKey() bool    { return len(c.idempotencyCandidates) != 0 }

func validateCandidateSets(lookups []IdempotencyLookupCandidate, lookupWrite uint32, digests []RequestDigestCandidate, digestWrite uint32) error {
	if len(lookups) == 0 {
		if lookupWrite != 0 || len(digests) != 0 || digestWrite != 0 {
			return fmt.Errorf("invalid schedule command")
		}
		return nil
	}
	if len(lookups) > MaxLookupCandidates || len(digests) == 0 || len(digests) > MaxDigestCandidates || lookupWrite == 0 || digestWrite == 0 {
		return fmt.Errorf("invalid schedule command")
	}
	lookupWriteFound := false
	for i, candidate := range lookups {
		if candidate.version == 0 || (i > 0 && lookups[i-1].version >= candidate.version) {
			return fmt.Errorf("invalid schedule command")
		}
		lookupWriteFound = lookupWriteFound || candidate.version == lookupWrite
	}
	digestWriteFound := false
	for i, candidate := range digests {
		if candidate.version == 0 || (i > 0 && digests[i-1].version >= candidate.version) {
			return fmt.Errorf("invalid schedule command")
		}
		digestWriteFound = digestWriteFound || candidate.version == digestWrite
	}
	if !lookupWriteFound || !digestWriteFound {
		return fmt.Errorf("invalid schedule command")
	}
	return nil
}

type ReservationRef struct {
	id         string
	generation uint64
	capability []byte
}

func NewReservationRef(id string, generation uint64, capability []byte) (ReservationRef, error) {
	if !boundedToken(id, 128) || generation == 0 || len(capability) != 32 {
		return ReservationRef{}, ErrInvalidReference
	}
	return ReservationRef{id: id, generation: generation, capability: append([]byte(nil), capability...)}, nil
}

func (r ReservationRef) ID() string         { return r.id }
func (r ReservationRef) Generation() uint64 { return r.generation }
func (r ReservationRef) Capability() []byte { return append([]byte(nil), r.capability...) }
func (r ReservationRef) String() string     { return "reservation[redacted]" }

type Reservation struct{ ref ReservationRef }

func NewReservation(ref ReservationRef) Reservation { return Reservation{ref: ref} }
func (r Reservation) Ref() ReservationRef           { return r.ref }

type WorkloadIdentityParams struct {
	Cluster       string
	Namespace     string
	LogicalEngine string
	PodUID        string
	EndpointEpoch uint64
	RecoveryEpoch uint64
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

func (i WorkloadIdentity) Cluster() string       { return i.cluster }
func (i WorkloadIdentity) Namespace() string     { return i.namespace }
func (i WorkloadIdentity) LogicalEngine() string { return i.logicalEngine }
func (i WorkloadIdentity) PodUID() string        { return i.podUID }
func (i WorkloadIdentity) EndpointEpoch() uint64 { return i.endpointEpoch }
func (i WorkloadIdentity) RecoveryEpoch() uint64 { return i.recoveryEpoch }

func (i WorkloadIdentity) SPIFFEID(trustDomain string) (*url.URL, error) {
	if !boundedToken(trustDomain, 255) || strings.ContainsAny(trustDomain, "/?#") {
		return nil, fmt.Errorf("invalid trust domain")
	}
	return url.Parse("spiffe://" + trustDomain + "/cluster/" + url.PathEscape(i.cluster) + "/ns/" + url.PathEscape(i.namespace) + "/engine/" + url.PathEscape(i.logicalEngine) + "/pod/" + url.PathEscape(i.podUID) + "/epoch/" + fmt.Sprint(i.endpointEpoch) + "/recovery/" + fmt.Sprint(i.recoveryEpoch))
}

type DispatchTarget struct {
	endpoint string
	identity WorkloadIdentity
}

func NewDispatchTarget(endpoint string, identity WorkloadIdentity) (DispatchTarget, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return DispatchTarget{}, fmt.Errorf("invalid dispatch endpoint")
	}
	return DispatchTarget{endpoint: strings.TrimSuffix(endpoint, "/"), identity: identity}, nil
}

func (t DispatchTarget) Endpoint() string           { return t.endpoint }
func (t DispatchTarget) Identity() WorkloadIdentity { return t.identity }

type TerminalProof uint8

const (
	TerminalProofProviderFinish TerminalProof = iota + 1
	TerminalProofNotSent
)

type AmbiguousCause uint8

const (
	AmbiguousTransport AmbiguousCause = iota + 1
	AmbiguousCanceled
	AmbiguousProtocol
)

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

func boundedToken(value string, max int) bool {
	return value != "" && len(value) <= max && value == strings.TrimSpace(value)
}
