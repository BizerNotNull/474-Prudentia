package scheduling_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/BizerNotNull/474-Prudentia/internal/scheduling"
)

type snapshotOptions struct {
	pod           string
	model         string
	capabilities  domain.FeatureSet
	health        domain.HealthState
	drain         domain.DrainState
	configured    uint32
	reserved      uint32
	orphaned      uint32
	requiredAge   time.Duration
	load          *uint16
	loadAge       time.Duration
	cacheExpiry   time.Time
	projection    uint64
	endpointEpoch uint64
	recoveryEpoch uint64
}

func command(t *testing.T, features domain.FeatureSet, slotCost uint32) domain.ScheduleCommand {
	t.Helper()
	digest, err := domain.NewRequestDigestCandidate(1, bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := domain.NewScheduleCommand(domain.ScheduleParams{
		RequestID:          "request-1",
		AttemptID:          "attempt-1",
		Tenant:             "tenant-1",
		Model:              "model-1",
		SlotCost:           slotCost,
		Features:           features,
		ExecutionBudget:    time.Second,
		DigestCandidates:   []domain.RequestDigestCandidate{digest},
		DigestWriteVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return cmd
}

func policy(t *testing.T, preferCache bool) domain.PlacementPolicy {
	t.Helper()
	value, err := domain.NewPlacementPolicy(domain.PlacementPolicyParams{
		Version:        1,
		RequiredHealth: domain.HealthStateHealthy,
		MaxSnapshotAge: 30 * time.Second,
		PreferCache:    preferCache,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func snapshot(t *testing.T, asOf time.Time, options snapshotOptions) domain.InstanceSnapshot {
	t.Helper()
	if options.pod == "" {
		options.pod = "pod-default"
	}
	if options.model == "" {
		options.model = "model-1"
	}
	if !options.capabilities.Valid() {
		options.capabilities = domain.EmptyFeatureSet()
	}
	if options.health == 0 {
		options.health = domain.HealthStateHealthy
	}
	if options.drain == 0 {
		options.drain = domain.DrainStateReady
	}
	if options.configured == 0 {
		options.configured = 4
	}
	if options.requiredAge == 0 {
		options.requiredAge = time.Second
	}
	if options.projection == 0 {
		options.projection = 1
	}
	if options.endpointEpoch == 0 {
		options.endpointEpoch = 1
	}
	if options.recoveryEpoch == 0 {
		options.recoveryEpoch = 1
	}

	identity, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{
		Cluster: "cluster-a", Namespace: "inference", LogicalEngine: "engine-a",
		PodUID: options.pod, EndpointEpoch: options.endpointEpoch, RecoveryEpoch: options.recoveryEpoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, _ := domain.NewEndpointRef("https://" + options.pod + ".test")
	model, _ := domain.NewModelKey(options.model)
	fingerprint, _ := domain.NewModelFingerprint(model, "revision-1")
	structuralSource, _ := domain.NewSourceStamp(domain.SourceStructural, 1, 1)
	healthSource, _ := domain.NewSourceStamp(domain.SourceRuntimeHealth, 1, 1)
	structural, err := domain.NewStoredSourceStamp(domain.StoredSourceStampParams{Source: structuralSource, Identity: identity, Version: 1, AcceptedAt: asOf.Add(-options.requiredAge), ExpiresAt: asOf.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	health, err := domain.NewStoredSourceStamp(domain.StoredSourceStampParams{Source: healthSource, Identity: identity, Version: 1, AcceptedAt: asOf.Add(-options.requiredAge), ExpiresAt: asOf.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	params := domain.SnapshotParams{
		Identity: identity, Endpoint: endpoint, Model: fingerprint, Capabilities: options.capabilities,
		Structural: structural, Health: health, HealthState: options.health, DrainState: options.drain,
		ConfiguredSlots: options.configured, ReservedSlots: options.reserved, OrphanedSlots: options.orphaned,
		ProjectionVersion: options.projection, CatalogAsOf: asOf,
	}
	if options.load != nil {
		loadSource, _ := domain.NewSourceStamp(domain.SourceLoad, 1, 1)
		loadAge := options.loadAge
		if loadAge == 0 {
			loadAge = time.Second
		}
		params.Load, err = domain.NewStoredSourceStamp(domain.StoredSourceStampParams{Source: loadSource, Identity: identity, Version: 1, AcceptedAt: asOf.Add(-loadAge), ExpiresAt: asOf.Add(time.Minute)})
		if err != nil {
			t.Fatal(err)
		}
		params.HasLoadStamp = true
		params.AdvisoryLoad, err = domain.NewAdvisoryLoad(*options.load)
		if err != nil {
			t.Fatal(err)
		}
		params.HasAdvisoryLoad = true
	}
	if !options.cacheExpiry.IsZero() {
		hint, err := domain.NewCacheHint(domain.CacheHintParams{Identity: identity, Digest: [32]byte(bytes.Repeat([]byte{7}, 32)), ExpiresAt: options.cacheExpiry})
		if err != nil {
			t.Fatal(err)
		}
		params.CacheHints = []domain.CacheHint{hint}
	}
	value, err := domain.NewInstanceSnapshot(params)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func catalog(t *testing.T, asOf time.Time, candidates ...domain.InstanceSnapshot) domain.CandidateCatalog {
	t.Helper()
	value, err := domain.NewCandidateCatalog(candidates, asOf)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestRankIsPermutationInvariantWithStableIdentityTieBreak(t *testing.T) {
	asOf := time.Now().Add(-time.Minute).UTC()
	cmd := command(t, domain.EmptyFeatureSet(), 1)
	podA := snapshot(t, asOf, snapshotOptions{pod: "pod-a"})
	podB := snapshot(t, asOf, snapshotOptions{pod: "pod-b"})

	first, firstExplanation := scheduling.Rank(cmd, catalog(t, asOf, podB, podA), policy(t, false))
	second, secondExplanation := scheduling.Rank(cmd, catalog(t, asOf, podA, podB), policy(t, false))
	if len(first) != 2 || len(second) != 2 {
		t.Fatalf("ranked lengths = %d and %d, want 2", len(first), len(second))
	}
	for i := range first {
		if first[i].Snapshot().Identity() != second[i].Snapshot().Identity() || first[i].Score() != second[i].Score() || first[i].Rank() != uint32(i+1) {
			t.Fatalf("ranking changed at position %d", i)
		}
	}
	if first[0].Snapshot().Identity().PodUID() != "pod-a" || first[1].Snapshot().Identity().PodUID() != "pod-b" {
		t.Fatal("identity tie break is not canonical")
	}
	if !reflect.DeepEqual(firstExplanation.Rejections(), secondExplanation.Rejections()) || firstExplanation.Eligible() != secondExplanation.Eligible() {
		t.Fatal("explanation changed with catalog permutation")
	}
}

func TestRankHardFilterMatrix(t *testing.T) {
	asOf := time.Now().Add(-time.Minute).UTC()
	required, _ := domain.NewFeatureSet(domain.FeatureVersion1, 1<<domain.FeatureStreaming)
	cmd := command(t, required, 2)
	candidates := []domain.InstanceSnapshot{
		snapshot(t, asOf, snapshotOptions{pod: "a-model", model: "other", capabilities: required}),
		snapshot(t, asOf, snapshotOptions{pod: "b-features"}),
		snapshot(t, asOf, snapshotOptions{pod: "c-health", capabilities: required, health: domain.HealthStateDegraded}),
		snapshot(t, asOf, snapshotOptions{pod: "d-drain", capabilities: required, drain: domain.DrainStateActive}),
		snapshot(t, asOf, snapshotOptions{pod: "e-capacity", capabilities: required, configured: 3, reserved: 1, orphaned: 1}),
		snapshot(t, asOf, snapshotOptions{pod: "f-stale", capabilities: required, requiredAge: time.Minute}),
		snapshot(t, asOf, snapshotOptions{pod: "g-eligible", capabilities: required}),
	}

	ranked, explanation := scheduling.Rank(cmd, catalog(t, asOf, candidates...), policy(t, false))
	if len(ranked) != 1 || ranked[0].Snapshot().Identity().PodUID() != "g-eligible" {
		t.Fatalf("eligible candidates = %#v", ranked)
	}
	want := []domain.PlacementRejection{
		domain.PlacementRejectedModel,
		domain.PlacementRejectedFeatures,
		domain.PlacementRejectedHealth,
		domain.PlacementRejectedDrain,
		domain.PlacementRejectedCapacity,
		domain.PlacementRejectedStale,
	}
	if explanation.Evaluated() != 7 || explanation.Eligible() != 1 || !reflect.DeepEqual(explanation.Rejections(), want) {
		t.Fatalf("explanation = evaluated %d eligible %d rejections %v", explanation.Evaluated(), explanation.Eligible(), explanation.Rejections())
	}
}

func TestRankStaleOptionalEvidenceNeverImprovesScore(t *testing.T) {
	asOf := time.Now().Add(-time.Minute).UTC()
	cmd := command(t, domain.EmptyFeatureSet(), 1)
	lowLoad := uint16(500)
	fresh := snapshot(t, asOf, snapshotOptions{pod: "fresh", load: &lowLoad})
	cached := snapshot(t, asOf, snapshotOptions{pod: "cached", cacheExpiry: asOf.Add(time.Minute)})
	stale := snapshot(t, asOf, snapshotOptions{pod: "stale", load: &lowLoad, loadAge: time.Minute, cacheExpiry: asOf})
	unknown := snapshot(t, asOf, snapshotOptions{pod: "unknown"})

	ranked, _ := scheduling.Rank(cmd, catalog(t, asOf, stale, fresh, cached, unknown), policy(t, true))
	if len(ranked) != 4 || ranked[0].Snapshot().Identity().PodUID() != "cached" || ranked[1].Snapshot().Identity().PodUID() != "fresh" {
		t.Fatalf("unexpected optional evidence order: %#v", ranked)
	}
	var staleScore, unknownScore int64
	for _, candidate := range ranked {
		switch candidate.Snapshot().Identity().PodUID() {
		case "stale":
			staleScore = candidate.Score()
		case "unknown":
			unknownScore = candidate.Score()
		}
	}
	if staleScore != unknownScore {
		t.Fatalf("stale optional evidence score %d, unknown score %d", staleScore, unknownScore)
	}
}

type fakeStore struct {
	catalogs       []domain.CandidateCatalog
	candidateReads int
	lookups        []lookupResult
	lookupCalls    int
	reserveErrors  []error
	reserveCalls   []domain.WorkloadIdentity
	reservation    domain.Reservation
	prepareCalls   int
	abandonCalls   int
	giveUpCalls    int
	finalizeCalls  int
	ambiguousCalls int
}

type lookupResult struct {
	reservation domain.Reservation
	found       bool
	err         error
}

func (s *fakeStore) Candidates(context.Context, domain.ScheduleCommand) (domain.CandidateCatalog, error) {
	index := s.candidateReads
	s.candidateReads++
	if index >= len(s.catalogs) {
		index = len(s.catalogs) - 1
	}
	return s.catalogs[index], nil
}
func (s *fakeStore) LookupReservation(context.Context, domain.ScheduleCommand) (domain.Reservation, bool, error) {
	index := s.lookupCalls
	s.lookupCalls++
	if index >= len(s.lookups) {
		return domain.Reservation{}, false, nil
	}
	result := s.lookups[index]
	return result.reservation, result.found, result.err
}
func (s *fakeStore) TryReserve(_ context.Context, _ domain.ScheduleCommand, identity domain.WorkloadIdentity) (domain.Reservation, error) {
	s.reserveCalls = append(s.reserveCalls, identity)
	index := len(s.reserveCalls) - 1
	if index < len(s.reserveErrors) && s.reserveErrors[index] != nil {
		return domain.Reservation{}, s.reserveErrors[index]
	}
	return s.reservation, nil
}
func (s *fakeStore) PrepareDispatch(context.Context, domain.ReservationRef) (domain.DispatchTarget, error) {
	s.prepareCalls++
	return domain.DispatchTarget{}, nil
}
func (s *fakeStore) AbandonNeverDispatched(context.Context, domain.ReservationRef, domain.RerankReason) error {
	s.abandonCalls++
	return nil
}
func (s *fakeStore) GiveUpNeverDispatched(context.Context, domain.ReservationRef, domain.GiveUpReason) error {
	s.giveUpCalls++
	return nil
}
func (s *fakeStore) ReleaseTerminal(context.Context, domain.ReservationRef, domain.TerminalProof) error {
	s.finalizeCalls++
	return nil
}
func (s *fakeStore) ConvertToOrphanDebt(context.Context, domain.ReservationRef, domain.AmbiguousCause) error {
	s.ambiguousCalls++
	return nil
}

func reservation(t *testing.T, id string) domain.Reservation {
	t.Helper()
	ref, err := domain.NewReservationRef(id, 1, bytes.Repeat([]byte{3}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return domain.NewReservation(ref)
}

func TestScheduleRereadsAndReranksOnCatalogConflict(t *testing.T) {
	asOf := time.Now().Add(-time.Minute).UTC()
	cmd := command(t, domain.EmptyFeatureSet(), 1)
	result := reservation(t, "reservation-1")
	store := &fakeStore{
		catalogs: []domain.CandidateCatalog{
			catalog(t, asOf, snapshot(t, asOf, snapshotOptions{pod: "pod-a"})),
			catalog(t, asOf, snapshot(t, asOf, snapshotOptions{pod: "pod-b"})),
		},
		reserveErrors: []error{domain.ErrStaleTarget, nil},
		reservation:   result,
	}
	service, err := scheduling.NewService(store, 2, policy(t, false))
	if err != nil {
		t.Fatal(err)
	}
	got, err := service.Schedule(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if got.Ref().ID() != result.Ref().ID() || store.candidateReads != 2 || len(store.reserveCalls) != 2 || store.reserveCalls[0].PodUID() != "pod-a" || store.reserveCalls[1].PodUID() != "pod-b" {
		t.Fatalf("reread/rerank calls = reads %d reserves %v", store.candidateReads, store.reserveCalls)
	}
}

func TestScheduleSameAttemptRecoveryAndExhaustionPreserveGrant(t *testing.T) {
	asOf := time.Now().Add(-time.Minute).UTC()
	cmd := command(t, domain.EmptyFeatureSet(), 1)
	recovered := reservation(t, "recovered")

	t.Run("existing same-attempt reservation", func(t *testing.T) {
		store := &fakeStore{lookups: []lookupResult{{reservation: recovered, found: true}}}
		service, _ := scheduling.NewService(store, 2, policy(t, false))
		got, err := service.Schedule(context.Background(), cmd)
		if err != nil || got.Ref().ID() != "recovered" || store.candidateReads != 0 {
			t.Fatalf("recovery = %v, %v, candidate reads %d", got.Ref().ID(), err, store.candidateReads)
		}
	})

	t.Run("lost reserve response recovered after bounded conflict", func(t *testing.T) {
		store := &fakeStore{
			catalogs:      []domain.CandidateCatalog{catalog(t, asOf, snapshot(t, asOf, snapshotOptions{pod: "pod-a"}))},
			reserveErrors: []error{domain.ErrStaleTarget},
			lookups:       []lookupResult{{}, {reservation: recovered, found: true}},
		}
		service, _ := scheduling.NewService(store, 1, policy(t, false))
		got, err := service.Schedule(context.Background(), cmd)
		if err != nil || got.Ref().ID() != "recovered" {
			t.Fatalf("post-conflict recovery = %v, %v", got.Ref().ID(), err)
		}
	})

	t.Run("exhaustion does not guess terminal intent", func(t *testing.T) {
		store := &fakeStore{catalogs: []domain.CandidateCatalog{catalog(t, asOf)}}
		service, _ := scheduling.NewService(store, 2, policy(t, false))
		_, err := service.Schedule(context.Background(), cmd)
		if !errors.Is(err, domain.ErrNoCapacity) || store.abandonCalls != 0 || store.giveUpCalls != 0 || store.finalizeCalls != 0 {
			t.Fatalf("exhaustion = %v, mutation counts abandon=%d giveup=%d finalize=%d", err, store.abandonCalls, store.giveUpCalls, store.finalizeCalls)
		}
	})
}

func TestMutationMethodsValidateEvidenceAndSeparateTerminalSemantics(t *testing.T) {
	store := &fakeStore{}
	service, err := scheduling.NewService(store, 1, policy(t, false))
	if err != nil {
		t.Fatal(err)
	}
	ref := reservation(t, "reservation-1").Ref()
	ctx := context.Background()

	if err := service.AbandonBeforeDispatch(ctx, ref, domain.RerankStaleTarget); err != nil {
		t.Fatal(err)
	}
	if store.abandonCalls != 1 || store.giveUpCalls != 0 {
		t.Fatal("nonterminal abandon used terminal store transition")
	}
	if err := service.GiveUpBeforeDispatch(ctx, ref, domain.GiveUpReranksExhausted); err != nil {
		t.Fatal(err)
	}
	if store.giveUpCalls != 1 {
		t.Fatal("terminal give-up was not delegated")
	}
	if err := service.AbandonBeforeDispatch(ctx, ref, domain.RerankReason(99)); !errors.Is(err, domain.ErrInvalidState) || store.abandonCalls != 1 {
		t.Fatal("invalid rerank reason reached store")
	}
	if err := service.GiveUpBeforeDispatch(ctx, ref, domain.GiveUpReason(99)); !errors.Is(err, domain.ErrInvalidState) || store.giveUpCalls != 1 {
		t.Fatal("invalid give-up reason reached store")
	}

	for _, proof := range []domain.TerminalProof{domain.TerminalProofProviderFinish, domain.TerminalProofCompleteNonStreaming, domain.TerminalProofNotSent, domain.TerminalProofAuthenticatedProviderTermination} {
		if err := service.Finalize(ctx, ref, proof); err != nil {
			t.Fatalf("proof %d rejected: %v", proof, err)
		}
	}
	if err := service.Finalize(ctx, ref, 0); !errors.Is(err, domain.ErrInvalidState) || store.finalizeCalls != 4 {
		t.Fatal("invalid terminal proof reached store")
	}
	for _, cause := range []domain.AmbiguousCause{domain.AmbiguousTransport, domain.AmbiguousCanceled, domain.AmbiguousProtocol} {
		if err := service.MarkAmbiguous(ctx, ref, cause); err != nil {
			t.Fatalf("cause %d rejected: %v", cause, err)
		}
	}
	if err := service.MarkAmbiguous(ctx, ref, 0); !errors.Is(err, domain.ErrInvalidState) || store.ambiguousCalls != 3 {
		t.Fatal("invalid ambiguous cause reached store")
	}
	if _, err := service.PrepareDispatch(ctx, domain.ReservationRef{}); !errors.Is(err, domain.ErrInvalidReference) || store.prepareCalls != 0 {
		t.Fatal("invalid reference reached prepare store method")
	}
}
