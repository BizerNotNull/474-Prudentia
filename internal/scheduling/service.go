package scheduling

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

const (
	defaultSnapshotAge = time.Minute
	// One additional authoritative slot always outweighs all optional evidence.
	optionalScoreRange = int64(20_001)
	capacityScoreUnit  = optionalScoreRange + 1
	cacheScore         = int64(10_001)
)

// Store is the inward scheduling ledger and catalog boundary. Implementations
// validate reservation state and evidence atomically with their mutations.
type Store interface {
	Candidates(context.Context, domain.ScheduleCommand) (domain.CandidateCatalog, error)
	LookupReservation(context.Context, domain.ScheduleCommand) (domain.Reservation, bool, error)
	TryReserve(context.Context, domain.ScheduleCommand, domain.WorkloadIdentity) (domain.Reservation, error)
	PrepareDispatch(context.Context, domain.ReservationRef) (domain.DispatchTarget, error)
	AbandonNeverDispatched(context.Context, domain.ReservationRef, domain.RerankReason) error
	GiveUpNeverDispatched(context.Context, domain.ReservationRef, domain.GiveUpReason) error
	ReleaseTerminal(context.Context, domain.ReservationRef, domain.TerminalProof) error
	ConvertToOrphanDebt(context.Context, domain.ReservationRef, domain.AmbiguousCause) error
}

type Service struct {
	store         Store
	maxRerankRead int
	policy        domain.PlacementPolicy
}

// NewService accepts an optional policy only as a source-compatible bridge for
// existing composition roots. Ranking itself has one policy contract: Rank.
func NewService(store Store, maxRerankRead int, configured ...domain.PlacementPolicy) (*Service, error) {
	if store == nil || maxRerankRead < 1 || maxRerankRead > 8 || len(configured) > 1 {
		return nil, errors.New("invalid scheduling service configuration")
	}
	var policy domain.PlacementPolicy
	if len(configured) == 1 {
		policy = configured[0]
	} else {
		var err error
		policy, err = domain.NewPlacementPolicy(domain.PlacementPolicyParams{
			Version:        1,
			RequiredHealth: domain.HealthStateHealthy,
			MaxSnapshotAge: defaultSnapshotAge,
		})
		if err != nil {
			return nil, err
		}
	}
	if policy.Version() != 1 {
		return nil, errors.New("invalid scheduling service configuration")
	}
	return &Service{store: store, maxRerankRead: maxRerankRead, policy: policy}, nil
}

// Rank is a pure deterministic placement decision over one immutable,
// database-timestamped catalog. Required facts fail closed. Optional evidence
// contributes only while fresh and can never make a stale/unknown candidate
// score better.
func Rank(spec domain.ScheduleCommand, catalog domain.CandidateCatalog, policy domain.PlacementPolicy) ([]domain.RankedCandidate, domain.PlacementExplanation) {
	type scored struct {
		snapshot domain.InstanceSnapshot
		score    int64
	}

	asOf := catalog.AsOf()
	candidates := catalog.Candidates()
	eligible := make([]scored, 0, len(candidates))
	rejections := make([]domain.PlacementRejection, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Model().Model() != spec.ModelKey() {
			rejections = append(rejections, domain.PlacementRejectedModel)
			continue
		}
		if !candidate.Capabilities().Contains(spec.Features()) {
			rejections = append(rejections, domain.PlacementRejectedFeatures)
			continue
		}
		if candidate.HealthState() != policy.RequiredHealth() {
			rejections = append(rejections, domain.PlacementRejectedHealth)
			continue
		}
		if candidate.DrainState() != domain.DrainStateReady {
			rejections = append(rejections, domain.PlacementRejectedDrain)
			continue
		}
		if candidate.AvailableSlots() < spec.SlotCost() {
			rejections = append(rejections, domain.PlacementRejectedCapacity)
			continue
		}
		if staleRequiredEvidence(candidate, asOf, policy.MaxSnapshotAge()) {
			rejections = append(rejections, domain.PlacementRejectedStale)
			continue
		}

		score := int64(candidate.AvailableSlots()) * capacityScoreUnit
		if load, ok := freshLoad(candidate, asOf, policy.MaxSnapshotAge()); ok {
			score += int64(10_000 - load.UtilizationBasisPoints())
		}
		if policy.PreferCache() && hasFreshDigestHint(candidate, spec, asOf) {
			score += cacheScore
		}
		eligible = append(eligible, scored{snapshot: candidate, score: score})
	}

	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].score != eligible[j].score {
			return eligible[i].score > eligible[j].score
		}
		return identityLess(eligible[i].snapshot.Identity(), eligible[j].snapshot.Identity())
	})

	ranked := make([]domain.RankedCandidate, 0, len(eligible))
	for i, candidate := range eligible {
		value, err := domain.NewRankedCandidate(candidate.snapshot, candidate.score, uint32(i+1))
		if err != nil {
			continue // impossible for a snapshot admitted by CandidateCatalog
		}
		ranked = append(ranked, value)
	}
	explanation, _ := domain.NewPlacementExplanation(domain.PlacementExplanationParams{
		PolicyVersion: policy.Version(),
		Evaluated:     uint32(len(candidates)),
		Eligible:      uint32(len(ranked)),
		Rejections:    rejections,
	})
	return ranked, explanation
}

func staleRequiredEvidence(candidate domain.InstanceSnapshot, asOf time.Time, maxAge time.Duration) bool {
	if asOf.IsZero() || candidate.CatalogAsOf() != asOf {
		return true
	}
	for _, stamp := range []domain.StoredSourceStamp{candidate.StructuralStamp(), candidate.HealthStamp()} {
		if !stamp.FreshAt(asOf) || asOf.Sub(stamp.AcceptedAt()) > maxAge {
			return true
		}
	}
	return false
}

func freshLoad(candidate domain.InstanceSnapshot, asOf time.Time, maxAge time.Duration) (domain.AdvisoryLoad, bool) {
	stamp, stamped := candidate.LoadStamp()
	load, observed := candidate.AdvisoryLoad()
	if !stamped || !observed || !stamp.FreshAt(asOf) || asOf.Sub(stamp.AcceptedAt()) > maxAge {
		return domain.AdvisoryLoad{}, false
	}
	return load, true
}

func hasFreshDigestHint(candidate domain.InstanceSnapshot, spec domain.ScheduleCommand, asOf time.Time) bool {
	digests := spec.DigestCandidates()
	for _, hint := range candidate.CacheHints() {
		expiresAt, ok := hint.ExpiresAt()
		if !ok || !asOf.Before(expiresAt) {
			continue
		}
		for _, digest := range digests {
			if hint.Digest() == digest.Value() {
				return true
			}
		}
	}
	return false
}

func identityLess(left, right domain.WorkloadIdentity) bool {
	if left.Cluster() != right.Cluster() {
		return left.Cluster() < right.Cluster()
	}
	if left.Namespace() != right.Namespace() {
		return left.Namespace() < right.Namespace()
	}
	if left.LogicalEngine() != right.LogicalEngine() {
		return left.LogicalEngine() < right.LogicalEngine()
	}
	if left.PodUID() != right.PodUID() {
		return left.PodUID() < right.PodUID()
	}
	if left.EndpointEpoch() != right.EndpointEpoch() {
		return left.EndpointEpoch() < right.EndpointEpoch()
	}
	return left.RecoveryEpoch() < right.RecoveryEpoch()
}

func (s *Service) Schedule(ctx context.Context, cmd domain.ScheduleCommand) (domain.Reservation, error) {
	if reservation, found, err := s.store.LookupReservation(ctx, cmd); err != nil || found {
		return reservation, err
	}

	for range s.maxRerankRead {
		catalog, err := s.store.Candidates(ctx, cmd)
		if err != nil {
			return domain.Reservation{}, err
		}
		ranked, _ := Rank(cmd, catalog, s.policy)
		if len(ranked) == 0 {
			break
		}

		conflict := false
		for _, candidate := range ranked {
			reservation, err := s.store.TryReserve(ctx, cmd, candidate.Snapshot().Identity())
			if err == nil {
				return reservation, nil
			}
			if errors.Is(err, domain.ErrNoCapacity) || errors.Is(err, domain.ErrStaleTarget) {
				conflict = true
				break
			}
			return domain.Reservation{}, err
		}
		if !conflict {
			break
		}
	}

	// This second lookup recovers a commit whose response was lost. Failure to
	// place never guesses terminal intent and therefore never releases a
	// retained rerank grant.
	if reservation, found, err := s.store.LookupReservation(ctx, cmd); err != nil || found {
		return reservation, err
	}
	return domain.Reservation{}, domain.ErrNoCapacity
}

func (s *Service) PrepareDispatch(ctx context.Context, ref domain.ReservationRef) (domain.DispatchTarget, error) {
	if !validRef(ref) {
		return domain.DispatchTarget{}, domain.ErrInvalidReference
	}
	return s.store.PrepareDispatch(ctx, ref)
}

func (s *Service) AbandonBeforeDispatch(ctx context.Context, ref domain.ReservationRef, reason domain.RerankReason) error {
	if !validRef(ref) {
		return domain.ErrInvalidReference
	}
	if reason != domain.RerankStaleTarget {
		return domain.ErrInvalidState
	}
	return s.store.AbandonNeverDispatched(ctx, ref, reason)
}

func (s *Service) GiveUpBeforeDispatch(ctx context.Context, ref domain.ReservationRef, reason domain.GiveUpReason) error {
	if !validRef(ref) {
		return domain.ErrInvalidReference
	}
	if reason < domain.GiveUpCanceled || reason > domain.GiveUpReranksExhausted {
		return domain.ErrInvalidState
	}
	return s.store.GiveUpNeverDispatched(ctx, ref, reason)
}

func (s *Service) Finalize(ctx context.Context, ref domain.ReservationRef, proof domain.TerminalProof) error {
	if !validRef(ref) {
		return domain.ErrInvalidReference
	}
	if !proof.Valid() {
		return domain.ErrInvalidState
	}
	return s.store.ReleaseTerminal(ctx, ref, proof)
}

func (s *Service) MarkAmbiguous(ctx context.Context, ref domain.ReservationRef, cause domain.AmbiguousCause) error {
	if !validRef(ref) {
		return domain.ErrInvalidReference
	}
	if cause < domain.AmbiguousTransport || cause > domain.AmbiguousProtocol {
		return domain.ErrInvalidState
	}
	return s.store.ConvertToOrphanDebt(ctx, ref, cause)
}

func validRef(ref domain.ReservationRef) bool {
	return ref.ID() != "" && ref.Generation() != 0 && len(ref.Capability()) != 0 && len(ref.Capability()) <= domain.MaxCapabilityBytes
}
