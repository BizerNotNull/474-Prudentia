package schedulerclient

import (
	"context"
	"errors"
	"testing"
	"time"

	schedulerv1 "github.com/BizerNotNull/474-Prudentia/api/scheduler/v1"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type rpcStub struct {
	scheduleCalls, ambiguousCalls int
	captured                      []*schedulerv1.ScheduleRequest
}

func (s *rpcStub) Schedule(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
	panic("use scheduleFn")
}
func (s *rpcStub) PrepareDispatch(context.Context, *schedulerv1.PrepareDispatchRequest, ...grpc.CallOption) (*schedulerv1.PrepareDispatchResponse, error) {
	return nil, errors.New("unused")
}
func (s *rpcStub) AbandonBeforeDispatch(context.Context, *schedulerv1.AbandonBeforeDispatchRequest, ...grpc.CallOption) (*schedulerv1.Empty, error) {
	return nil, errors.New("unused")
}
func (s *rpcStub) GiveUpBeforeDispatch(context.Context, *schedulerv1.GiveUpBeforeDispatchRequest, ...grpc.CallOption) (*schedulerv1.Empty, error) {
	return nil, errors.New("unused")
}
func (s *rpcStub) Finalize(context.Context, *schedulerv1.FinalizeRequest, ...grpc.CallOption) (*schedulerv1.Empty, error) {
	return nil, errors.New("unused")
}
func (s *rpcStub) MarkAmbiguous(context.Context, *schedulerv1.MarkAmbiguousRequest, ...grpc.CallOption) (*schedulerv1.Empty, error) {
	s.ambiguousCalls++
	return nil, status.Error(codes.Unavailable, "down")
}

type retryRPC struct{ rpcStub }

func (s *retryRPC) Schedule(_ context.Context, request *schedulerv1.ScheduleRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
	s.scheduleCalls++
	s.captured = append(s.captured, proto.Clone(request).(*schedulerv1.ScheduleRequest))
	if s.scheduleCalls == 1 {
		return nil, status.Error(codes.Unavailable, "lost response")
	}
	return &schedulerv1.ScheduleResponse{SchemaVersion: 1, Reservation: &schedulerv1.Reservation{SchemaVersion: 1, Ref: &schedulerv1.ReservationRef{ReservationId: "r", Generation: 1, Capability: []byte("0123456789abcdef"), SchemaVersion: 1}}}, nil
}

func scheduleCommand(t *testing.T) domain.ScheduleCommand {
	t.Helper()
	digest, _ := domain.NewRequestDigestCandidate(1, make([]byte, 32))
	features, _ := domain.NewFeatureSet(domain.FeatureVersion1, 0)
	command, err := domain.NewScheduleCommand(domain.ScheduleParams{RequestID: "request", AttemptID: "attempt", Tenant: "tenant", DigestCandidates: []domain.RequestDigestCandidate{digest}, DigestWriteVersion: 1, Model: "model", SlotCost: 1, Features: features, Priority: domain.PriorityHigh, CachePolicy: domain.CachePolicyRequireCompatible, ExecutionBudget: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return command
}

func TestScheduleRetriesIdenticalBoundedRequest(t *testing.T) {
	rpc := &retryRPC{}
	client, err := NewWithConfig(rpc, Config{MaxAttempts: 2, RetryDelay: 0, CleanupTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := client.Schedule(context.Background(), scheduleCommand(t))
	if err != nil {
		t.Fatal(err)
	}
	if reservation.Ref().ID() != "r" || rpc.scheduleCalls != 2 {
		t.Fatalf("reservation=%v calls=%d", reservation.Ref(), rpc.scheduleCalls)
	}
	if !proto.Equal(rpc.captured[0], rpc.captured[1]) {
		t.Fatal("retry changed command bytes")
	}
	if rpc.captured[0].SchemaVersion != 1 || rpc.captured[0].Features.SchemaVersion != 1 || rpc.captured[0].Priority != schedulerv1.Priority_PRIORITY_HIGH || rpc.captured[0].CachePolicy != schedulerv1.CachePolicy_CACHE_POLICY_REQUIRE_COMPATIBLE {
		t.Fatal("client omitted strict wire versions")
	}
}

func TestMarkAmbiguousNeverReplaysProviderRelatedWork(t *testing.T) {
	rpc := &retryRPC{}
	client, _ := NewWithConfig(rpc, Config{MaxAttempts: 3, RetryDelay: 0, CleanupTimeout: time.Second})
	ref, _ := domain.NewReservationRef("r", 1, []byte("0123456789abcdef"))
	if err := client.MarkAmbiguous(context.Background(), ref, domain.AmbiguousTransport); err == nil {
		t.Fatal("expected unavailable")
	}
	if rpc.ambiguousCalls != 1 {
		t.Fatalf("ambiguous calls = %d", rpc.ambiguousCalls)
	}
}
