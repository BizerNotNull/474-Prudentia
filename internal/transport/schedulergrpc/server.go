package schedulergrpc

import (
	"context"
	"errors"

	schedulerv1 "github.com/BizerNotNull/474-Prudentia/api/scheduler/v1"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type Scheduler interface {
	Schedule(context.Context, domain.ScheduleCommand) (domain.Reservation, error)
	PrepareDispatch(context.Context, domain.ReservationRef) (domain.DispatchTarget, error)
	AbandonBeforeDispatch(context.Context, domain.ReservationRef, domain.RerankReason) error
	GiveUpBeforeDispatch(context.Context, domain.ReservationRef, domain.GiveUpReason) error
	Finalize(context.Context, domain.ReservationRef, domain.TerminalProof) error
	MarkAmbiguous(context.Context, domain.ReservationRef, domain.AmbiguousCause) error
}

type Server struct {
	schedulerv1.UnimplementedSchedulerServiceServer
	scheduler Scheduler
	codec     Codec
}

func NewServer(scheduler Scheduler) (*Server, error) {
	if scheduler == nil {
		return nil, errors.New("scheduler is required")
	}
	return &Server{scheduler: scheduler}, nil
}

func (s *Server) Schedule(ctx context.Context, request *schedulerv1.ScheduleRequest) (*schedulerv1.ScheduleResponse, error) {
	command, err := s.codec.DecodeSchedule(request)
	if err != nil {
		return nil, invalidArgument(s.codec)
	}
	reservation, err := s.scheduler.Schedule(ctx, command)
	if err != nil {
		return nil, transportError(s.codec, err)
	}
	encoded, err := s.codec.EncodeReservation(reservation)
	if err != nil {
		return nil, transportError(s.codec, err)
	}
	return &schedulerv1.ScheduleResponse{Reservation: encoded, SchemaVersion: SchemaVersionV1}, nil
}

func (s *Server) PrepareDispatch(ctx context.Context, request *schedulerv1.PrepareDispatchRequest) (*schedulerv1.PrepareDispatchResponse, error) {
	if request == nil || requireV1(request.SchemaVersion) != nil {
		return nil, invalidArgument(s.codec)
	}
	ref, err := s.codec.DecodeReservationRef(request.Ref)
	if err != nil {
		return nil, invalidArgument(s.codec)
	}
	target, err := s.scheduler.PrepareDispatch(ctx, ref)
	if err != nil {
		return nil, transportError(s.codec, err)
	}
	identity := target.Identity()
	if len(target.Endpoint()) > MaxEndpointBytes {
		return nil, transportError(s.codec, errors.New("invalid dispatch target"))
	}
	return &schedulerv1.PrepareDispatchResponse{SchemaVersion: SchemaVersionV1, Target: &schedulerv1.DispatchTarget{Endpoint: target.Endpoint(), SchemaVersion: SchemaVersionV1, Identity: &schedulerv1.WorkloadIdentity{Cluster: identity.Cluster(), Namespace: identity.Namespace(), LogicalEngine: identity.LogicalEngine(), PodUid: identity.PodUID(), EndpointEpoch: identity.EndpointEpoch(), RecoveryEpoch: identity.RecoveryEpoch(), SchemaVersion: SchemaVersionV1}}}, nil
}

func (s *Server) AbandonBeforeDispatch(ctx context.Context, request *schedulerv1.AbandonBeforeDispatchRequest) (*schedulerv1.Empty, error) {
	if request == nil || requireV1(request.SchemaVersion) != nil || request.Reason != schedulerv1.RerankReason_RERANK_REASON_STALE_TARGET {
		return nil, invalidArgument(s.codec)
	}
	ref, err := s.codec.DecodeReservationRef(request.Ref)
	if err != nil {
		return nil, invalidArgument(s.codec)
	}
	if err := s.scheduler.AbandonBeforeDispatch(ctx, ref, domain.RerankStaleTarget); err != nil {
		return nil, transportError(s.codec, err)
	}
	return &schedulerv1.Empty{}, nil
}

func (s *Server) GiveUpBeforeDispatch(ctx context.Context, request *schedulerv1.GiveUpBeforeDispatchRequest) (*schedulerv1.Empty, error) {
	if request == nil || requireV1(request.SchemaVersion) != nil {
		return nil, invalidArgument(s.codec)
	}
	ref, err := s.codec.DecodeReservationRef(request.Ref)
	if err != nil {
		return nil, invalidArgument(s.codec)
	}
	var reason domain.GiveUpReason
	switch request.Reason {
	case schedulerv1.GiveUpReason_GIVE_UP_REASON_CANCELED:
		reason = domain.GiveUpCanceled
	case schedulerv1.GiveUpReason_GIVE_UP_REASON_BUDGET_EXPIRED:
		reason = domain.GiveUpBudgetExpired
	case schedulerv1.GiveUpReason_GIVE_UP_REASON_RERANKS_EXHAUSTED:
		reason = domain.GiveUpReranksExhausted
	default:
		return nil, invalidArgument(s.codec)
	}
	if err := s.scheduler.GiveUpBeforeDispatch(ctx, ref, reason); err != nil {
		return nil, transportError(s.codec, err)
	}
	return &schedulerv1.Empty{}, nil
}

func (s *Server) Finalize(ctx context.Context, request *schedulerv1.FinalizeRequest) (*schedulerv1.Empty, error) {
	if request == nil || requireV1(request.SchemaVersion) != nil || requireV1(request.TerminalEvidenceVersion) != nil {
		return nil, invalidArgument(s.codec)
	}
	ref, err := s.codec.DecodeReservationRef(request.Ref)
	if err != nil {
		return nil, invalidArgument(s.codec)
	}
	var proof domain.TerminalProof
	switch request.Proof {
	case schedulerv1.TerminalProof_TERMINAL_PROOF_PROVIDER_FINISH:
		proof = domain.TerminalProofProviderFinish
	case schedulerv1.TerminalProof_TERMINAL_PROOF_COMPLETE_NON_STREAMING:
		proof = domain.TerminalProofCompleteNonStreaming
	case schedulerv1.TerminalProof_TERMINAL_PROOF_NOT_SENT:
		proof = domain.TerminalProofNotSent
	case schedulerv1.TerminalProof_TERMINAL_PROOF_AUTHENTICATED_PROVIDER_TERMINATION:
		proof = domain.TerminalProofAuthenticatedProviderTermination
	default:
		return nil, invalidArgument(s.codec)
	}
	if err := s.scheduler.Finalize(ctx, ref, proof); err != nil {
		return nil, transportError(s.codec, err)
	}
	return &schedulerv1.Empty{}, nil
}

func (s *Server) MarkAmbiguous(ctx context.Context, request *schedulerv1.MarkAmbiguousRequest) (*schedulerv1.Empty, error) {
	if request == nil || requireV1(request.SchemaVersion) != nil || requireV1(request.EvidenceSchemaVersion) != nil {
		return nil, invalidArgument(s.codec)
	}
	ref, err := s.codec.DecodeReservationRef(request.Ref)
	if err != nil {
		return nil, invalidArgument(s.codec)
	}
	var cause domain.AmbiguousCause
	switch request.Cause {
	case schedulerv1.AmbiguousCause_AMBIGUOUS_CAUSE_TRANSPORT:
		cause = domain.AmbiguousTransport
	case schedulerv1.AmbiguousCause_AMBIGUOUS_CAUSE_CANCELED:
		cause = domain.AmbiguousCanceled
	case schedulerv1.AmbiguousCause_AMBIGUOUS_CAUSE_PROTOCOL:
		cause = domain.AmbiguousProtocol
	default:
		return nil, invalidArgument(s.codec)
	}
	if err := s.scheduler.MarkAmbiguous(ctx, ref, cause); err != nil {
		return nil, transportError(s.codec, err)
	}
	return &schedulerv1.Empty{}, nil
}
