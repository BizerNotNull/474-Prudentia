package request_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	requestapp "github.com/BizerNotNull/474-Prudentia/internal/request"
	"github.com/BizerNotNull/474-Prudentia/internal/transport/publichttp"
)

type recordingScheduler struct {
	ref            domain.ReservationRef
	target         domain.DispatchTarget
	finalized      domain.TerminalProof
	ambiguous      domain.AmbiguousCause
	command        domain.ScheduleCommand
	prepareCalls   int
	giveUpCalls    int
	finalizeCalls  int
	ambiguousCalls int
}

func newRecordingScheduler(t *testing.T) *recordingScheduler {
	t.Helper()
	ref, err := domain.NewReservationRef("res_test", 1, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: "cluster", Namespace: "ns", LogicalEngine: "engine", PodUID: "pod", EndpointEpoch: 1, RecoveryEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	target, err := domain.NewDispatchTarget("https://provider.invalid", identity)
	if err != nil {
		t.Fatal(err)
	}
	return &recordingScheduler{ref: ref, target: target}
}
func (s *recordingScheduler) Schedule(_ context.Context, command domain.ScheduleCommand) (domain.Reservation, error) {
	s.command = command
	return domain.NewReservation(s.ref), nil
}
func (s *recordingScheduler) PrepareDispatch(context.Context, domain.ReservationRef) (domain.DispatchTarget, error) {
	s.prepareCalls++
	return s.target, nil
}
func (s *recordingScheduler) AbandonBeforeDispatch(context.Context, domain.ReservationRef, domain.RerankReason) error {
	return nil
}

func (s *recordingScheduler) GiveUpBeforeDispatch(context.Context, domain.ReservationRef, domain.GiveUpReason) error {
	s.giveUpCalls++
	return nil
}
func (s *recordingScheduler) Finalize(_ context.Context, _ domain.ReservationRef, proof domain.TerminalProof) error {
	s.finalized = proof
	s.finalizeCalls++
	return nil
}
func (s *recordingScheduler) MarkAmbiguous(_ context.Context, _ domain.ReservationRef, cause domain.AmbiguousCause) error {
	s.ambiguous = cause
	s.ambiguousCalls++
	return nil
}

type failingProvider struct{ err error }

func (p failingProvider) Infer(context.Context, domain.DispatchTarget, domain.AuthorizedRequest, publichttp.StreamSink) error {
	return p.err
}

type countingFailureProvider struct {
	calls int
	err   error
}

func (p *countingFailureProvider) Infer(context.Context, domain.DispatchTarget, domain.AuthorizedRequest, publichttp.StreamSink) error {
	p.calls++
	return p.err
}

type terminalThenDeliveryErrorProvider struct{ calls int }

func (p *terminalThenDeliveryErrorProvider) Infer(ctx context.Context, _ domain.DispatchTarget, _ domain.AuthorizedRequest, sink publichttp.StreamSink) error {
	p.calls++
	return sink.Write(ctx, domain.NewTerminalEvent())
}

type failingSink struct{ err error }

func (s failingSink) Write(context.Context, domain.StreamEvent) error { return s.err }

type recordingCache struct {
	calls int
	err   error
}

func (c *recordingCache) Prepare(context.Context, domain.ReservationRef, domain.AuthorizedRequest) error {
	c.calls++
	return c.err
}

type switchingScheduler struct {
	refs          []domain.ReservationRef
	target        domain.DispatchTarget
	scheduleCalls int
	prepareCalls  int
	abandoned     []uint64
	abandonReason domain.RerankReason
	givenUp       uint64
	giveUpReason  domain.GiveUpReason
	finalized     uint64
	alwaysStale   bool
}

func newSwitchingScheduler(t *testing.T, reservations int) *switchingScheduler {
	t.Helper()
	base := newRecordingScheduler(t)
	refs := make([]domain.ReservationRef, reservations)
	for i := range refs {
		ref, err := domain.NewReservationRef("res_switch", uint64(i+1), []byte("abcdef0123456789abcdef0123456789"))
		if err != nil {
			t.Fatal(err)
		}
		refs[i] = ref
	}
	return &switchingScheduler{refs: refs, target: base.target}
}

func (s *switchingScheduler) Schedule(context.Context, domain.ScheduleCommand) (domain.Reservation, error) {
	if s.scheduleCalls >= len(s.refs) {
		return domain.Reservation{}, domain.ErrNoCapacity
	}
	ref := s.refs[s.scheduleCalls]
	s.scheduleCalls++
	return domain.NewReservation(ref), nil
}

func (s *switchingScheduler) PrepareDispatch(context.Context, domain.ReservationRef) (domain.DispatchTarget, error) {
	s.prepareCalls++
	if s.alwaysStale || s.prepareCalls == 1 {
		return domain.DispatchTarget{}, domain.ErrStaleTarget
	}
	return s.target, nil
}

func (s *switchingScheduler) AbandonBeforeDispatch(_ context.Context, ref domain.ReservationRef, reason domain.RerankReason) error {
	s.abandoned = append(s.abandoned, ref.Generation())
	s.abandonReason = reason
	return nil
}

func (s *switchingScheduler) GiveUpBeforeDispatch(_ context.Context, ref domain.ReservationRef, reason domain.GiveUpReason) error {
	s.givenUp = ref.Generation()
	s.giveUpReason = reason
	return nil
}

func (s *switchingScheduler) Finalize(_ context.Context, ref domain.ReservationRef, _ domain.TerminalProof) error {
	s.finalized = ref.Generation()
	return nil
}

func (*switchingScheduler) MarkAmbiguous(context.Context, domain.ReservationRef, domain.AmbiguousCause) error {
	return nil
}

type countingProvider struct{ calls int }

func (p *countingProvider) Infer(context.Context, domain.DispatchTarget, domain.AuthorizedRequest, publichttp.StreamSink) error {
	p.calls++
	return nil
}

type discardSink struct{}

func (discardSink) Write(context.Context, domain.StreamEvent) error { return nil }

func authorizedRequest(t *testing.T) domain.AuthorizedRequest {
	return authorizedRequestWithCache(t, domain.CachePolicyDisabled)
}

func authorizedRequestWithCache(t *testing.T, policy domain.CachePolicy) domain.AuthorizedRequest {
	t.Helper()
	features, err := domain.NewFeatureSet(domain.FeatureVersion1, 1<<domain.FeaturePrefixCache)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := domain.NewPrincipalFromParams(domain.PrincipalParams{
		Subject: "subject", Tenant: mustTenant(t, "tenant"), Models: []domain.ModelKey{mustModel(t, "model")},
		Features: features, MaxPriority: domain.PriorityNormal, CachePolicies: []domain.CachePolicy{domain.CachePolicyDisabled, domain.CachePolicyPrefer, domain.CachePolicyRequireCompatible},
	})
	if err != nil {
		t.Fatal(err)
	}
	requestFeatures := domain.EmptyFeatureSet()
	if policy != domain.CachePolicyDisabled {
		requestFeatures = features
	}
	inference, err := domain.NewInferenceRequest(domain.InferenceRequestParams{
		Model: "model", Messages: []domain.MessageParams{{Role: "user", Content: "prompt"}},
		MaxCompletionTokens: 16, Priority: domain.PriorityNormal, Features: requestFeatures,
		CachePolicy: policy, ExecutionBudget: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	authorized, err := domain.NewCheckedAuthorizedRequest(principal, inference)
	if err != nil {
		t.Fatal(err)
	}
	return authorized
}

func mustTenant(t *testing.T, value string) domain.TenantScope {
	t.Helper()
	tenant, err := domain.NewTenantScope(value)
	if err != nil {
		t.Fatal(err)
	}
	return tenant
}

func mustModel(t *testing.T, value string) domain.ModelKey {
	t.Helper()
	model, err := domain.NewModelKey(value)
	if err != nil {
		t.Fatal(err)
	}
	return model
}

func idempotencyConfig() requestapp.IdempotencyConfig {
	return requestapp.IdempotencyConfig{
		LookupKeys:         []requestapp.VersionedKey{{Version: 1, Key: []byte("lookup-key-32-bytes-long-value!!")}},
		LookupWriteVersion: 1,
		DigestKeys:         []requestapp.VersionedKey{{Version: 1, Key: []byte("digest-key-32-bytes-long-value!!")}},
		DigestWriteVersion: 1,
	}
}

func TestValidateIdempotencyConfigUsesDomainCandidateBounds(t *testing.T) {
	config := idempotencyConfig()
	config.LookupKeys = make([]requestapp.VersionedKey, domain.MaxLookupCandidates)
	for i := range config.LookupKeys {
		config.LookupKeys[i] = requestapp.VersionedKey{
			Version: uint32(i + 1),
			Key:     []byte("lookup-key-32-bytes-long-value!!"),
		}
	}
	config.LookupWriteVersion = uint32(domain.MaxLookupCandidates)
	if _, err := requestapp.ValidateIdempotencyConfig(config); err != nil {
		t.Fatalf("maximum-size keyring rejected: %v", err)
	}
	config.LookupKeys = append(config.LookupKeys, requestapp.VersionedKey{
		Version: uint32(domain.MaxLookupCandidates + 1),
		Key:     []byte("lookup-key-32-bytes-long-value!!"),
	})
	if _, err := requestapp.ValidateIdempotencyConfig(config); err == nil {
		t.Fatal("oversize keyring accepted")
	}
}

func TestTransportFailureAfterBodyMayBeSentCreatesAmbiguousDebt(t *testing.T) {
	scheduler := newRecordingScheduler(t)
	service, err := requestapp.NewService(scheduler, failingProvider{err: errors.New("connection reset")}, idempotencyConfig(), time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Infer(context.Background(), "req_test", nil, authorizedRequest(t), domain.ResponseModeNonStreaming, discardSink{}); domain.ErrorKindOf(err) != domain.ErrorBackendUnavailable {
		t.Fatalf("error = %v, want backend unavailable", err)
	}
	if scheduler.ambiguous != domain.AmbiguousTransport || scheduler.finalized != 0 {
		t.Fatalf("ambiguous = %v, finalized = %v", scheduler.ambiguous, scheduler.finalized)
	}
}

func TestAmbiguousDispatchUsesOneCleanupTransitionAndNeverReposts(t *testing.T) {
	scheduler := newRecordingScheduler(t)
	provider := &countingFailureProvider{err: errors.New("connection reset")}
	service, err := requestapp.NewService(scheduler, provider, idempotencyConfig(), time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = service.Infer(context.Background(), "req_once", nil, authorizedRequest(t), domain.ResponseModeNonStreaming, discardSink{})
	if provider.calls != 1 || scheduler.ambiguousCalls != 1 || scheduler.finalizeCalls != 0 {
		t.Fatalf("provider=%d ambiguous=%d finalize=%d", provider.calls, scheduler.ambiguousCalls, scheduler.finalizeCalls)
	}
}
func TestDefinitiveNotSentFailureReleasesReservation(t *testing.T) {
	scheduler := newRecordingScheduler(t)
	service, err := requestapp.NewService(scheduler, failingProvider{err: requestapp.NewNotSentError(errors.New("identity mismatch"))}, idempotencyConfig(), time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = service.Infer(context.Background(), "req_test", nil, authorizedRequest(t), domain.ResponseModeNonStreaming, discardSink{})
	if scheduler.finalized != domain.TerminalProofNotSent || scheduler.ambiguous != 0 {
		t.Fatalf("finalized = %v, ambiguous = %v", scheduler.finalized, scheduler.ambiguous)
	}
}

func TestValidTerminalFinalizesBeforeDeliveryError(t *testing.T) {
	scheduler := newRecordingScheduler(t)
	provider := &terminalThenDeliveryErrorProvider{}
	service, err := requestapp.NewService(scheduler, provider, idempotencyConfig(), time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	deliveryErr := errors.New("client disconnected")
	err = service.Infer(context.Background(), "req_terminal_delivery", nil, authorizedRequest(t), domain.ResponseModeStreaming, failingSink{err: deliveryErr})
	if domain.ErrorKindOf(err) != domain.ErrorBackendUnavailable {
		t.Fatalf("error = %v, want backend unavailable", err)
	}
	if scheduler.finalized != domain.TerminalProofProviderFinish || scheduler.finalizeCalls != 1 || scheduler.ambiguousCalls != 0 || provider.calls != 1 {
		t.Fatalf("finalized=%v finalize calls=%d ambiguous calls=%d provider calls=%d", scheduler.finalized, scheduler.finalizeCalls, scheduler.ambiguousCalls, provider.calls)
	}
}

func TestCachePreparationPolicyBeforeDispatch(t *testing.T) {
	for _, test := range []struct {
		name         string
		policy       domain.CachePolicy
		withCache    bool
		cacheErr     error
		wantCalls    int
		wantPrepare  int
		wantGiveUp   int
		wantProvider int
		wantError    bool
	}{
		{name: "disabled cold default", policy: domain.CachePolicyDisabled, wantPrepare: 1, wantProvider: 1},
		{name: "prefer outage falls back cold", policy: domain.CachePolicyPrefer, withCache: true, cacheErr: errors.New("cache unavailable"), wantCalls: 1, wantPrepare: 1, wantProvider: 1},
		{name: "require without port fails closed", policy: domain.CachePolicyRequireCompatible, wantGiveUp: 1, wantError: true},
		{name: "require outage fails before prepare", policy: domain.CachePolicyRequireCompatible, withCache: true, cacheErr: errors.New("cache unavailable"), wantCalls: 1, wantGiveUp: 1, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheduler := newRecordingScheduler(t)
			provider := &countingProvider{}
			cache := &recordingCache{err: test.cacheErr}
			var service *requestapp.Service
			var err error
			if test.withCache {
				service, err = requestapp.NewService(scheduler, provider, idempotencyConfig(), time.Minute, time.Second, cache)
			} else {
				service, err = requestapp.NewService(scheduler, provider, idempotencyConfig(), time.Minute, time.Second)
			}
			if err != nil {
				t.Fatal(err)
			}
			err = service.Infer(context.Background(), "req_cache", nil, authorizedRequestWithCache(t, test.policy), domain.ResponseModeNonStreaming, discardSink{})
			if (err != nil) != test.wantError {
				t.Fatalf("error=%v wantError=%v", err, test.wantError)
			}
			if cache.calls != test.wantCalls || scheduler.prepareCalls != test.wantPrepare || scheduler.giveUpCalls != test.wantGiveUp || provider.calls != test.wantProvider {
				t.Fatalf("cache=%d prepare=%d giveup=%d provider=%d", cache.calls, scheduler.prepareCalls, scheduler.giveUpCalls, provider.calls)
			}
		})
	}
}

func TestInferDerivesBoundedCandidatesAndZeroizesRawIdempotencyKey(t *testing.T) {
	scheduler := newRecordingScheduler(t)
	service, err := requestapp.NewService(scheduler, failingProvider{err: requestapp.NewNotSentError(errors.New("not sent"))}, idempotencyConfig(), time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rawKey := []byte("client-operation-1")
	_ = service.Infer(context.Background(), "req_public", rawKey, authorizedRequest(t), domain.ResponseModeNonStreaming, discardSink{})
	if scheduler.command.RequestID() != "req_public" {
		t.Fatalf("request ID = %q", scheduler.command.RequestID())
	}
	if len(scheduler.command.IdempotencyCandidates()) != 1 || scheduler.command.LookupWriteVersion() != 1 {
		t.Fatalf("unexpected lookup candidate set")
	}
	if len(scheduler.command.DigestCandidates()) != 1 || scheduler.command.DigestWriteVersion() != 1 {
		t.Fatalf("unexpected digest candidate set")
	}
	for i, value := range rawKey {
		if value != 0 {
			t.Fatalf("raw key byte %d was not zeroized", i)
		}
	}
}

func TestInferIncludesDigestCandidatesWithoutIdempotencyKey(t *testing.T) {
	scheduler := newRecordingScheduler(t)
	service, err := requestapp.NewService(scheduler, failingProvider{err: requestapp.NewNotSentError(errors.New("not sent"))}, idempotencyConfig(), time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = service.Infer(context.Background(), "req_no_key", nil, authorizedRequest(t), domain.ResponseModeNonStreaming, discardSink{})
	if scheduler.command.HasIdempotencyKey() || len(scheduler.command.IdempotencyCandidates()) != 0 {
		t.Fatal("request without key unexpectedly had lookup candidates")
	}
	if len(scheduler.command.DigestCandidates()) != 1 || scheduler.command.DigestWriteVersion() != 1 {
		t.Fatal("request without key did not carry digest candidates")
	}
}

func TestInferPropagatesAuthorizedSchedulingPolicy(t *testing.T) {
	scheduler := newRecordingScheduler(t)
	cache := &recordingCache{}
	service, err := requestapp.NewService(scheduler, failingProvider{err: requestapp.NewNotSentError(errors.New("not sent"))}, idempotencyConfig(), time.Minute, time.Second, cache)
	if err != nil {
		t.Fatal(err)
	}
	authorized := authorizedRequestWithCache(t, domain.CachePolicyPrefer)
	_ = service.Infer(context.Background(), "req_policy", nil, authorized, domain.ResponseModeNonStreaming, discardSink{})
	command := scheduler.command
	if command.Priority() != authorized.Request().Priority() || command.CachePolicy() != domain.CachePolicyPrefer || command.Features().Bits() != authorized.Request().Features().Bits() {
		t.Fatalf("command priority=%v cache=%v features=%d", command.Priority(), command.CachePolicy(), command.Features().Bits())
	}
}

func TestInferAbandonsStaleCandidateAndDispatchesNextGeneration(t *testing.T) {
	scheduler := newSwitchingScheduler(t, 2)
	provider := &countingProvider{}
	service, err := requestapp.NewService(scheduler, provider, idempotencyConfig(), time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if err := service.Infer(context.Background(), "req_switch", nil, authorizedRequest(t), domain.ResponseModeNonStreaming, discardSink{}); err != nil {
		t.Fatalf("infer after candidate switch: %v", err)
	}
	if scheduler.scheduleCalls != 2 || scheduler.prepareCalls != 2 {
		t.Fatalf("schedule calls = %d, prepare calls = %d", scheduler.scheduleCalls, scheduler.prepareCalls)
	}
	if len(scheduler.abandoned) != 1 || scheduler.abandoned[0] != 1 || scheduler.abandonReason != domain.RerankStaleTarget {
		t.Fatalf("abandonment = %v, reason = %v", scheduler.abandoned, scheduler.abandonReason)
	}
	if scheduler.givenUp != 0 || scheduler.finalized != 2 || provider.calls != 1 {
		t.Fatalf("given up generation = %d, finalized generation = %d, provider calls = %d", scheduler.givenUp, scheduler.finalized, provider.calls)
	}
}

func TestInferBoundsCandidateSwitchesAndTerminallyGivesUpLatestRef(t *testing.T) {
	scheduler := newSwitchingScheduler(t, 4)
	scheduler.alwaysStale = true
	provider := &countingProvider{}
	service, err := requestapp.NewService(scheduler, provider, idempotencyConfig(), time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	err = service.Infer(context.Background(), "req_exhausted", nil, authorizedRequest(t), domain.ResponseModeNonStreaming, discardSink{})
	if domain.ErrorKindOf(err) != domain.ErrorRecoveryFenced {
		t.Fatalf("error = %v, want recovery fenced", err)
	}
	if scheduler.scheduleCalls != 4 || scheduler.prepareCalls != 4 || len(scheduler.abandoned) != 3 {
		t.Fatalf("schedule calls = %d, prepare calls = %d, abandonments = %v", scheduler.scheduleCalls, scheduler.prepareCalls, scheduler.abandoned)
	}
	if scheduler.givenUp != 4 || scheduler.giveUpReason != domain.GiveUpReranksExhausted {
		t.Fatalf("given up generation = %d, reason = %v", scheduler.givenUp, scheduler.giveUpReason)
	}
	if provider.calls != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.calls)
	}
}
