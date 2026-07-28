package schedulerclient

import (
	"context"
	"errors"

	schedulerv1 "github.com/BizerNotNull/474-Prudentia/api/scheduler/v1"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Client struct {
	rpc schedulerv1.SchedulerServiceClient
}

func New(rpc schedulerv1.SchedulerServiceClient) (*Client, error) {
	if rpc == nil {
		return nil, errors.New("scheduler RPC client is required")
	}
	return &Client{rpc: rpc}, nil
}

func (c *Client) Schedule(ctx context.Context, command domain.ScheduleCommand) (domain.Reservation, error) {
	lookupCandidates := command.IdempotencyCandidates()
	wireLookups := make([]*schedulerv1.IdempotencyLookupCandidate, len(lookupCandidates))
	for i, candidate := range lookupCandidates {
		value := candidate.Value()
		wireLookups[i] = &schedulerv1.IdempotencyLookupCandidate{PepperVersion: candidate.Version(), HmacSha256: value[:]}
	}
	digestCandidates := command.DigestCandidates()
	wireDigests := make([]*schedulerv1.RequestDigestCandidate, len(digestCandidates))
	for i, candidate := range digestCandidates {
		value := candidate.Value()
		wireDigests[i] = &schedulerv1.RequestDigestCandidate{DigestVersion: candidate.Version(), HmacSha256: value[:]}
	}
	response, err := c.rpc.Schedule(ctx, &schedulerv1.ScheduleRequest{
		RequestId: command.RequestID(), AttemptId: command.AttemptID(), TenantScope: []byte(command.Tenant()),
		IdempotencyLookupCandidates: wireLookups, LookupWriteVersion: command.LookupWriteVersion(),
		DigestCandidates: wireDigests, DigestWriteVersion: command.DigestWriteVersion(),
		Model: command.Model(), SlotCost: command.SlotCost(), ExecutionBudgetMs: command.ExecutionBudget().Milliseconds(),
	})
	if err != nil {
		return domain.Reservation{}, decodeError(err)
	}
	if response.GetReservation() == nil {
		return domain.Reservation{}, errors.New("scheduler returned no reservation")
	}
	ref, err := decodeRef(response.Reservation.Ref)
	if err != nil {
		return domain.Reservation{}, err
	}
	return domain.NewReservation(ref), nil
}

func (c *Client) PrepareDispatch(ctx context.Context, ref domain.ReservationRef) (domain.DispatchTarget, error) {
	response, err := c.rpc.PrepareDispatch(ctx, &schedulerv1.PrepareDispatchRequest{Ref: encodeRef(ref)})
	if err != nil {
		return domain.DispatchTarget{}, decodeError(err)
	}
	target := response.GetTarget()
	if target == nil || target.Identity == nil {
		return domain.DispatchTarget{}, errors.New("scheduler returned invalid dispatch target")
	}
	identityMessage := target.Identity
	identity, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{
		Cluster: identityMessage.Cluster, Namespace: identityMessage.Namespace, LogicalEngine: identityMessage.LogicalEngine,
		PodUID: identityMessage.PodUid, EndpointEpoch: identityMessage.EndpointEpoch, RecoveryEpoch: identityMessage.RecoveryEpoch,
	})
	if err != nil {
		return domain.DispatchTarget{}, errors.New("scheduler returned invalid workload identity")
	}
	return domain.NewDispatchTarget(target.Endpoint, identity)
}

func (c *Client) AbandonBeforeDispatch(ctx context.Context, ref domain.ReservationRef, reason domain.RerankReason) error {
	wireReason := schedulerv1.RerankReason_RERANK_REASON_UNSPECIFIED
	if reason == domain.RerankStaleTarget {
		wireReason = schedulerv1.RerankReason_RERANK_REASON_STALE_TARGET
	}
	_, err := c.rpc.AbandonBeforeDispatch(ctx, &schedulerv1.AbandonBeforeDispatchRequest{Ref: encodeRef(ref), Reason: wireReason})
	return decodeError(err)
}

func (c *Client) GiveUpBeforeDispatch(ctx context.Context, ref domain.ReservationRef, reason domain.GiveUpReason) error {
	_, err := c.rpc.GiveUpBeforeDispatch(ctx, &schedulerv1.GiveUpBeforeDispatchRequest{Ref: encodeRef(ref), Reason: schedulerv1.GiveUpReason(reason)})
	return decodeError(err)
}

func (c *Client) Finalize(ctx context.Context, ref domain.ReservationRef, proof domain.TerminalProof) error {
	wireProof := schedulerv1.TerminalProof_TERMINAL_PROOF_PROVIDER_FINISH
	if proof == domain.TerminalProofNotSent {
		wireProof = schedulerv1.TerminalProof_TERMINAL_PROOF_NOT_SENT
	}
	_, err := c.rpc.Finalize(ctx, &schedulerv1.FinalizeRequest{Ref: encodeRef(ref), Proof: wireProof})
	return decodeError(err)
}

func (c *Client) MarkAmbiguous(ctx context.Context, ref domain.ReservationRef, cause domain.AmbiguousCause) error {
	wireCause := schedulerv1.AmbiguousCause_AMBIGUOUS_CAUSE_TRANSPORT
	switch cause {
	case domain.AmbiguousCanceled:
		wireCause = schedulerv1.AmbiguousCause_AMBIGUOUS_CAUSE_CANCELED
	case domain.AmbiguousProtocol:
		wireCause = schedulerv1.AmbiguousCause_AMBIGUOUS_CAUSE_PROTOCOL
	}
	_, err := c.rpc.MarkAmbiguous(ctx, &schedulerv1.MarkAmbiguousRequest{Ref: encodeRef(ref), Cause: wireCause})
	return decodeError(err)
}

func encodeRef(ref domain.ReservationRef) *schedulerv1.ReservationRef {
	return &schedulerv1.ReservationRef{ReservationId: ref.ID(), Generation: ref.Generation(), Capability: ref.Capability()}
}

func decodeRef(message *schedulerv1.ReservationRef) (domain.ReservationRef, error) {
	if message == nil {
		return domain.ReservationRef{}, errors.New("scheduler returned invalid reservation")
	}
	return domain.NewReservationRef(message.ReservationId, message.Generation, message.Capability)
}

func decodeError(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.AlreadyExists:
		return domain.ErrIdempotencyConflict
	case codes.Aborted:
		return domain.ErrRequestInProgress
	case codes.NotFound:
		return domain.ErrRequestNotReplayable
	case codes.ResourceExhausted:
		return domain.ErrNoCapacity
	case codes.PermissionDenied:
		return domain.ErrInvalidReference
	case codes.OutOfRange:
		return domain.ErrStaleTarget

	case codes.FailedPrecondition:
		return domain.ErrInvalidState
	case codes.Canceled:
		return context.Canceled
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	case codes.Unavailable:
		return domain.NewPublicError(domain.ErrorUnavailable)
	default:
		return errors.New("scheduler request failed")
	}
}
