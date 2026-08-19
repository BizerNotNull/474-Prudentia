package contract_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"testing"
	"time"

	schedulerv1 "github.com/BizerNotNull/474-Prudentia/api/scheduler/v1"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/BizerNotNull/474-Prudentia/internal/transport/schedulergrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type rejectingScheduler struct{ scheduleCalls int }

func (s *rejectingScheduler) Schedule(context.Context, domain.ScheduleCommand) (domain.Reservation, error) {
	s.scheduleCalls++
	return domain.Reservation{}, domain.ErrNoCapacity
}
func (*rejectingScheduler) PrepareDispatch(context.Context, domain.ReservationRef) (domain.DispatchTarget, error) {
	return domain.DispatchTarget{}, domain.ErrInvalidState
}
func (*rejectingScheduler) AbandonBeforeDispatch(context.Context, domain.ReservationRef, domain.RerankReason) error {
	return domain.ErrInvalidState
}
func (*rejectingScheduler) GiveUpBeforeDispatch(context.Context, domain.ReservationRef, domain.GiveUpReason) error {
	return domain.ErrInvalidState
}
func (*rejectingScheduler) Finalize(context.Context, domain.ReservationRef, domain.TerminalProof) error {
	return domain.ErrInvalidState
}
func (*rejectingScheduler) MarkAmbiguous(context.Context, domain.ReservationRef, domain.AmbiguousCause) error {
	return domain.ErrInvalidState
}

func validScheduleRequest() *schedulerv1.ScheduleRequest {
	lookup := sha256.Sum256([]byte("lookup"))
	digest := sha256.Sum256([]byte("digest"))
	return &schedulerv1.ScheduleRequest{
		RequestId: "request-1", AttemptId: "attempt-1", TenantScope: []byte("tenant-a"),
		IdempotencyLookupCandidates: []*schedulerv1.IdempotencyLookupCandidate{{PepperVersion: 1, HmacSha256: lookup[:]}},
		LookupWriteVersion:          1,
		DigestCandidates:            []*schedulerv1.RequestDigestCandidate{{DigestVersion: 1, HmacSha256: digest[:]}},
		DigestWriteVersion:          1, Model: "model-a", SlotCost: 1, ExecutionBudgetMs: int64(time.Minute / time.Millisecond),
		Features: &schedulerv1.FeatureSet{SchemaVersion: 1}, Priority: schedulerv1.Priority_PRIORITY_NORMAL,
		SchemaVersion: 1, BudgetSchemaVersion: 1,
	}
}

func TestSchedulerProtobufDriftFailsBeforeDomainDispatch(t *testing.T) {
	cases := map[string]func(*schedulerv1.ScheduleRequest){
		"unknown request schema": func(r *schedulerv1.ScheduleRequest) { r.SchemaVersion = 2 },
		"unknown budget schema":  func(r *schedulerv1.ScheduleRequest) { r.BudgetSchemaVersion = 2 },
		"unknown feature schema": func(r *schedulerv1.ScheduleRequest) { r.Features.SchemaVersion = 2 },
		"unknown feature bit":    func(r *schedulerv1.ScheduleRequest) { r.Features.Bits = 1 << 63 },
		"unknown priority":       func(r *schedulerv1.ScheduleRequest) { r.Priority = schedulerv1.Priority(99) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			backend := &rejectingScheduler{}
			server, err := schedulergrpc.NewServer(backend)
			if err != nil {
				t.Fatal(err)
			}
			request := validScheduleRequest()
			mutate(request)
			_, err = server.Schedule(context.Background(), request)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
			}
			if backend.scheduleCalls != 0 {
				t.Fatalf("invalid wire value reached scheduler %d time(s)", backend.scheduleCalls)
			}
		})
	}
}

func TestSerializedScheduleCarriesDerivedCandidatesNeverRawSecret(t *testing.T) {
	rawKey := []byte("raw-client-idempotency-key-never-on-wire")
	lookupKey := []byte("tenant-separated-lookup-pepper")
	digestKey := []byte("tenant-separated-digest-key")
	lookupMAC := hmac.New(sha256.New, lookupKey)
	lookupMAC.Write([]byte("tenant-a\x00"))
	lookupMAC.Write(rawKey)
	digestMAC := hmac.New(sha256.New, digestKey)
	digestMAC.Write([]byte("tenant-a\x00"))
	digestMAC.Write([]byte("canonical-request"))
	request := validScheduleRequest()
	request.IdempotencyLookupCandidates[0].HmacSha256 = lookupMAC.Sum(nil)
	request.DigestCandidates[0].HmacSha256 = digestMAC.Sum(nil)
	wire, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(wire, rawKey) {
		t.Fatal("serialized scheduler command leaked raw idempotency key")
	}
	if !bytes.Contains(wire, request.IdempotencyLookupCandidates[0].HmacSha256) {
		t.Fatal("serialized command omitted derived lookup candidate")
	}
	if !bytes.Contains(wire, request.DigestCandidates[0].HmacSha256) {
		t.Fatal("serialized command omitted derived digest candidate")
	}
}
