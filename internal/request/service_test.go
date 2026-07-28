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
	ref       domain.ReservationRef
	target    domain.DispatchTarget
	finalized domain.TerminalProof
	ambiguous domain.AmbiguousCause
	command   domain.ScheduleCommand
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
	return s.target, nil
}
func (s *recordingScheduler) GiveUpBeforeDispatch(context.Context, domain.ReservationRef, domain.GiveUpReason) error {
	return nil
}
func (s *recordingScheduler) Finalize(_ context.Context, _ domain.ReservationRef, proof domain.TerminalProof) error {
	s.finalized = proof
	return nil
}
func (s *recordingScheduler) MarkAmbiguous(_ context.Context, _ domain.ReservationRef, cause domain.AmbiguousCause) error {
	s.ambiguous = cause
	return nil
}

type failingProvider struct{ err error }

func (p failingProvider) Infer(context.Context, domain.DispatchTarget, domain.AuthorizedRequest, publichttp.StreamSink) error {
	return p.err
}

type discardSink struct{}

func (discardSink) Write(context.Context, domain.StreamEvent) error { return nil }

func authorizedRequest(t *testing.T) domain.AuthorizedRequest {
	t.Helper()
	principal, err := domain.NewPrincipal("tenant", []string{"model"})
	if err != nil {
		t.Fatal(err)
	}
	inference, err := domain.NewInferenceRequest(domain.InferenceRequestParams{Model: "model", Messages: []domain.MessageParams{{Role: "user", Content: "prompt"}}, MaxCompletionTokens: 16})
	if err != nil {
		t.Fatal(err)
	}
	return domain.NewAuthorizedRequest(principal, inference)
}

func idempotencyConfig() requestapp.IdempotencyConfig {
	return requestapp.IdempotencyConfig{
		LookupKeys:         []requestapp.VersionedKey{{Version: 1, Key: []byte("lookup-key-32-bytes-long-value!!")}},
		LookupWriteVersion: 1,
		DigestKeys:         []requestapp.VersionedKey{{Version: 1, Key: []byte("digest-key-32-bytes-long-value!!")}},
		DigestWriteVersion: 1,
	}
}

func TestTransportFailureAfterBodyMayBeSentCreatesAmbiguousDebt(t *testing.T) {
	scheduler := newRecordingScheduler(t)
	service, err := requestapp.NewService(scheduler, failingProvider{err: errors.New("connection reset")}, idempotencyConfig(), time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Infer(context.Background(), "req_test", nil, authorizedRequest(t), domain.ResponseModeNonStreaming, discardSink{}); domain.ErrorKindOf(err) != domain.ErrorUnavailable {
		t.Fatalf("error = %v, want unavailable", err)
	}
	if scheduler.ambiguous != domain.AmbiguousTransport || scheduler.finalized != 0 {
		t.Fatalf("ambiguous = %v, finalized = %v", scheduler.ambiguous, scheduler.finalized)
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
