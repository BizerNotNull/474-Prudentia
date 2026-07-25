package schedulergrpc

import (
	"context"
	"errors"
	"time"

	schedulerv1 "github.com/BizerNotNull/474-Prudentia/api/scheduler/v1"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Scheduler interface {
	Schedule(context.Context, domain.ScheduleCommand) (domain.Reservation, error)
	PrepareDispatch(context.Context, domain.ReservationRef) (domain.DispatchTarget, error)
	GiveUpBeforeDispatch(context.Context, domain.ReservationRef, domain.GiveUpReason) error
	Finalize(context.Context, domain.ReservationRef, domain.TerminalProof) error
	MarkAmbiguous(context.Context, domain.ReservationRef, domain.AmbiguousCause) error
}

type Server struct {
	schedulerv1.UnimplementedSchedulerServiceServer
	scheduler Scheduler
}

func NewServer(scheduler Scheduler) (*Server, error) {
	if scheduler == nil {
		return nil, errors.New("scheduler is required")
	}
	return &Server{scheduler: scheduler}, nil
}

func (s *Server) Schedule(ctx context.Context, request *schedulerv1.ScheduleRequest) (*schedulerv1.ScheduleResponse, error) {
	if request == nil || request.ExecutionBudgetMs <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid schedule request")
	}
	command, err := domain.NewScheduleCommand(domain.ScheduleParams{
		RequestID: request.RequestId, AttemptID: request.AttemptId, Tenant: string(request.TenantScope),
		Model: request.Model, SlotCost: request.SlotCost, ExecutionBudget: time.Duration(request.ExecutionBudgetMs) * time.Millisecond,
	})
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid schedule request")
	}
	reservation, err := s.scheduler.Schedule(ctx, command)
	if err != nil {
		return nil, encodeError(err)
	}
	return &schedulerv1.ScheduleResponse{Reservation: encodeReservation(reservation)}, nil
}

func (s *Server) PrepareDispatch(ctx context.Context, request *schedulerv1.PrepareDispatchRequest) (*schedulerv1.PrepareDispatchResponse, error) {
	ref, err := decodeRef(request.GetRef())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid reservation reference")
	}
	target, err := s.scheduler.PrepareDispatch(ctx, ref)
	if err != nil {
		return nil, encodeError(err)
	}
	identity := target.Identity()
	return &schedulerv1.PrepareDispatchResponse{Target: &schedulerv1.DispatchTarget{
		Endpoint: target.Endpoint(),
		Identity: &schedulerv1.WorkloadIdentity{Cluster: identity.Cluster(), Namespace: identity.Namespace(), LogicalEngine: identity.LogicalEngine(), PodUid: identity.PodUID(), EndpointEpoch: identity.EndpointEpoch(), RecoveryEpoch: identity.RecoveryEpoch()},
	}}, nil
}

func (s *Server) GiveUpBeforeDispatch(ctx context.Context, request *schedulerv1.GiveUpBeforeDispatchRequest) (*schedulerv1.Empty, error) {
	ref, err := decodeRef(request.GetRef())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid reservation reference")
	}
	reason := domain.GiveUpReason(request.GetReason())
	if reason < domain.GiveUpCanceled || reason > domain.GiveUpReranksExhausted {
		return nil, status.Error(codes.InvalidArgument, "invalid give-up reason")
	}
	if err := s.scheduler.GiveUpBeforeDispatch(ctx, ref, reason); err != nil {
		return nil, encodeError(err)
	}
	return &schedulerv1.Empty{}, nil
}

func (s *Server) Finalize(ctx context.Context, request *schedulerv1.FinalizeRequest) (*schedulerv1.Empty, error) {
	ref, err := decodeRef(request.GetRef())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid reservation reference")
	}
	var proof domain.TerminalProof
	switch request.GetProof() {
	case schedulerv1.TerminalProof_TERMINAL_PROOF_PROVIDER_FINISH:
		proof = domain.TerminalProofProviderFinish
	case schedulerv1.TerminalProof_TERMINAL_PROOF_NOT_SENT:
		proof = domain.TerminalProofNotSent
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid terminal proof")
	}
	if err := s.scheduler.Finalize(ctx, ref, proof); err != nil {
		return nil, encodeError(err)
	}
	return &schedulerv1.Empty{}, nil
}

func (s *Server) MarkAmbiguous(ctx context.Context, request *schedulerv1.MarkAmbiguousRequest) (*schedulerv1.Empty, error) {
	ref, err := decodeRef(request.GetRef())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid reservation reference")
	}
	var cause domain.AmbiguousCause
	switch request.GetCause() {
	case schedulerv1.AmbiguousCause_AMBIGUOUS_CAUSE_TRANSPORT:
		cause = domain.AmbiguousTransport
	case schedulerv1.AmbiguousCause_AMBIGUOUS_CAUSE_CANCELED:
		cause = domain.AmbiguousCanceled
	case schedulerv1.AmbiguousCause_AMBIGUOUS_CAUSE_PROTOCOL:
		cause = domain.AmbiguousProtocol
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid ambiguity cause")
	}
	if err := s.scheduler.MarkAmbiguous(ctx, ref, cause); err != nil {
		return nil, encodeError(err)
	}
	return &schedulerv1.Empty{}, nil
}

func encodeReservation(reservation domain.Reservation) *schedulerv1.Reservation {
	ref := reservation.Ref()
	return &schedulerv1.Reservation{Ref: &schedulerv1.ReservationRef{ReservationId: ref.ID(), Generation: ref.Generation(), Capability: ref.Capability()}}
}

func decodeRef(message *schedulerv1.ReservationRef) (domain.ReservationRef, error) {
	if message == nil {
		return domain.ReservationRef{}, domain.ErrInvalidReference
	}
	return domain.NewReservationRef(message.ReservationId, message.Generation, message.Capability)
}

func encodeError(err error) error {
	switch {
	case errors.Is(err, domain.ErrNoCapacity):
		return status.Error(codes.ResourceExhausted, "no schedulable capacity")
	case errors.Is(err, domain.ErrInvalidReference):
		return status.Error(codes.PermissionDenied, "invalid reservation reference")
	case errors.Is(err, domain.ErrInvalidState), errors.Is(err, domain.ErrStaleTarget):
		return status.Error(codes.FailedPrecondition, "reservation cannot transition")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	default:
		return status.Error(codes.Internal, "scheduler operation failed")
	}
}
