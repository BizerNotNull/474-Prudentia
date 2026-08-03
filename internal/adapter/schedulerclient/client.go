package schedulerclient

import (
	"context"
	"errors"
	"time"

	schedulerv1 "github.com/BizerNotNull/474-Prudentia/api/scheduler/v1"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const schemaVersionV1 uint32 = 1

type Config struct {
	MaxAttempts                int
	RetryDelay, CleanupTimeout time.Duration
}
type Client struct {
	rpc    schedulerv1.SchedulerServiceClient
	config Config
}

func New(rpc schedulerv1.SchedulerServiceClient) (*Client, error) {
	return NewWithConfig(rpc, Config{MaxAttempts: 3, RetryDelay: 5 * time.Millisecond, CleanupTimeout: 2 * time.Second})
}
func NewWithConfig(rpc schedulerv1.SchedulerServiceClient, config Config) (*Client, error) {
	if rpc == nil {
		return nil, errors.New("scheduler RPC client is required")
	}
	if config.MaxAttempts < 1 || config.MaxAttempts > 5 || config.RetryDelay < 0 || config.RetryDelay > time.Second || config.CleanupTimeout <= 0 || config.CleanupTimeout > 30*time.Second {
		return nil, errors.New("invalid scheduler client configuration")
	}
	return &Client{rpc: rpc, config: config}, nil
}

func (c *Client) Schedule(ctx context.Context, command domain.ScheduleCommand) (domain.Reservation, error) {
	lookups := command.IdempotencyCandidates()
	wireLookups := make([]*schedulerv1.IdempotencyLookupCandidate, len(lookups))
	for i, candidate := range lookups {
		value := candidate.Value()
		wireLookups[i] = &schedulerv1.IdempotencyLookupCandidate{PepperVersion: candidate.Version(), HmacSha256: append([]byte(nil), value[:]...)}
	}
	digests := command.DigestCandidates()
	wireDigests := make([]*schedulerv1.RequestDigestCandidate, len(digests))
	for i, candidate := range digests {
		value := candidate.Value()
		wireDigests[i] = &schedulerv1.RequestDigestCandidate{DigestVersion: candidate.Version(), HmacSha256: append([]byte(nil), value[:]...)}
	}
	features := command.Features()
	request := &schedulerv1.ScheduleRequest{RequestId: command.RequestID(), AttemptId: command.AttemptID(), TenantScope: []byte(command.Tenant()), IdempotencyLookupCandidates: wireLookups, LookupWriteVersion: command.LookupWriteVersion(), DigestCandidates: wireDigests, DigestWriteVersion: command.DigestWriteVersion(), Model: command.Model(), SlotCost: command.SlotCost(), ExecutionBudgetMs: command.ExecutionBudget().Milliseconds(), Features: &schedulerv1.FeatureSet{SchemaVersion: schemaVersionV1, Bits: features.Bits()}, Priority: encodePriority(command.Priority()), CachePolicy: encodeCachePolicy(command.CachePolicy()), SchemaVersion: schemaVersionV1, BudgetSchemaVersion: schemaVersionV1}
	var response *schedulerv1.ScheduleResponse
	err := c.retry(ctx, func(callCtx context.Context) error {
		var err error
		response, err = c.rpc.Schedule(callCtx, request)
		return err
	})
	if err != nil {
		return domain.Reservation{}, decodeError(err)
	}
	if response == nil || response.SchemaVersion != schemaVersionV1 || response.Reservation == nil || response.Reservation.SchemaVersion != schemaVersionV1 {
		return domain.Reservation{}, errors.New("scheduler returned unsupported reservation")
	}
	ref, err := decodeRef(response.Reservation.Ref)
	if err != nil {
		return domain.Reservation{}, err
	}
	return domain.NewReservation(ref), nil
}

func encodePriority(priority domain.Priority) schedulerv1.Priority {
	switch priority {
	case domain.PriorityBackground:
		return schedulerv1.Priority_PRIORITY_BACKGROUND
	case domain.PriorityNormal:
		return schedulerv1.Priority_PRIORITY_NORMAL
	case domain.PriorityHigh:
		return schedulerv1.Priority_PRIORITY_HIGH
	default:
		return schedulerv1.Priority_PRIORITY_UNSPECIFIED
	}
}

func encodeCachePolicy(policy domain.CachePolicy) schedulerv1.CachePolicy {
	switch policy {
	case domain.CachePolicyDisabled:
		return schedulerv1.CachePolicy_CACHE_POLICY_DISABLED
	case domain.CachePolicyPrefer:
		return schedulerv1.CachePolicy_CACHE_POLICY_PREFER
	case domain.CachePolicyRequireCompatible:
		return schedulerv1.CachePolicy_CACHE_POLICY_REQUIRE_COMPATIBLE
	default:
		return schedulerv1.CachePolicy_CACHE_POLICY_UNSPECIFIED
	}
}

func (c *Client) PrepareDispatch(ctx context.Context, ref domain.ReservationRef) (domain.DispatchTarget, error) {
	request := &schedulerv1.PrepareDispatchRequest{Ref: encodeRef(ref), SchemaVersion: schemaVersionV1}
	var response *schedulerv1.PrepareDispatchResponse
	err := c.retry(ctx, func(callCtx context.Context) error {
		var err error
		response, err = c.rpc.PrepareDispatch(callCtx, request)
		return err
	})
	if err != nil {
		return domain.DispatchTarget{}, decodeError(err)
	}
	if response == nil || response.SchemaVersion != schemaVersionV1 {
		return domain.DispatchTarget{}, errors.New("scheduler returned unsupported dispatch response")
	}
	target := response.Target
	if target == nil || target.SchemaVersion != schemaVersionV1 || target.Identity == nil || target.Identity.SchemaVersion != schemaVersionV1 || len(target.Endpoint) > 2048 {
		return domain.DispatchTarget{}, errors.New("scheduler returned invalid dispatch target")
	}
	identity, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: target.Identity.Cluster, Namespace: target.Identity.Namespace, LogicalEngine: target.Identity.LogicalEngine, PodUID: target.Identity.PodUid, EndpointEpoch: target.Identity.EndpointEpoch, RecoveryEpoch: target.Identity.RecoveryEpoch})
	if err != nil {
		return domain.DispatchTarget{}, errors.New("scheduler returned invalid workload identity")
	}
	return domain.NewDispatchTarget(target.Endpoint, identity)
}

func (c *Client) AbandonBeforeDispatch(ctx context.Context, ref domain.ReservationRef, reason domain.RerankReason) error {
	if reason != domain.RerankStaleTarget {
		return errors.New("invalid rerank reason")
	}
	request := &schedulerv1.AbandonBeforeDispatchRequest{Ref: encodeRef(ref), Reason: schedulerv1.RerankReason_RERANK_REASON_STALE_TARGET, SchemaVersion: schemaVersionV1}
	return decodeError(c.retry(ctx, func(callCtx context.Context) error {
		_, err := c.rpc.AbandonBeforeDispatch(callCtx, request)
		return err
	}))
}

func (c *Client) GiveUpBeforeDispatch(ctx context.Context, ref domain.ReservationRef, reason domain.GiveUpReason) error {
	var wire schedulerv1.GiveUpReason
	switch reason {
	case domain.GiveUpCanceled:
		wire = schedulerv1.GiveUpReason_GIVE_UP_REASON_CANCELED
	case domain.GiveUpBudgetExpired:
		wire = schedulerv1.GiveUpReason_GIVE_UP_REASON_BUDGET_EXPIRED
	case domain.GiveUpReranksExhausted:
		wire = schedulerv1.GiveUpReason_GIVE_UP_REASON_RERANKS_EXHAUSTED
	default:
		return errors.New("invalid give-up reason")
	}
	cleanup, cancel := c.cleanupContext(ctx)
	defer cancel()
	request := &schedulerv1.GiveUpBeforeDispatchRequest{Ref: encodeRef(ref), Reason: wire, SchemaVersion: schemaVersionV1}
	return decodeError(c.retry(cleanup, func(callCtx context.Context) error {
		_, err := c.rpc.GiveUpBeforeDispatch(callCtx, request)
		return err
	}))
}

func (c *Client) Finalize(ctx context.Context, ref domain.ReservationRef, proof domain.TerminalProof) error {
	var wire schedulerv1.TerminalProof
	switch proof {
	case domain.TerminalProofProviderFinish:
		wire = schedulerv1.TerminalProof_TERMINAL_PROOF_PROVIDER_FINISH
	case domain.TerminalProofCompleteNonStreaming:
		wire = schedulerv1.TerminalProof_TERMINAL_PROOF_COMPLETE_NON_STREAMING
	case domain.TerminalProofNotSent:
		wire = schedulerv1.TerminalProof_TERMINAL_PROOF_NOT_SENT
	case domain.TerminalProofAuthenticatedProviderTermination:
		wire = schedulerv1.TerminalProof_TERMINAL_PROOF_AUTHENTICATED_PROVIDER_TERMINATION
	default:
		return errors.New("invalid terminal proof")
	}
	cleanup, cancel := c.cleanupContext(ctx)
	defer cancel()
	request := &schedulerv1.FinalizeRequest{Ref: encodeRef(ref), Proof: wire, TerminalEvidenceVersion: schemaVersionV1, SchemaVersion: schemaVersionV1}
	return decodeError(c.retry(cleanup, func(callCtx context.Context) error { _, err := c.rpc.Finalize(callCtx, request); return err }))
}

func (c *Client) MarkAmbiguous(ctx context.Context, ref domain.ReservationRef, cause domain.AmbiguousCause) error {
	var wire schedulerv1.AmbiguousCause
	switch cause {
	case domain.AmbiguousTransport:
		wire = schedulerv1.AmbiguousCause_AMBIGUOUS_CAUSE_TRANSPORT
	case domain.AmbiguousCanceled:
		wire = schedulerv1.AmbiguousCause_AMBIGUOUS_CAUSE_CANCELED
	case domain.AmbiguousProtocol:
		wire = schedulerv1.AmbiguousCause_AMBIGUOUS_CAUSE_PROTOCOL
	default:
		return errors.New("invalid ambiguity cause")
	}
	request := &schedulerv1.MarkAmbiguousRequest{Ref: encodeRef(ref), Cause: wire, EvidenceSchemaVersion: schemaVersionV1, SchemaVersion: schemaVersionV1}
	_, err := c.rpc.MarkAmbiguous(ctx, request) // Never delay debt conversion behind transport replay.
	return decodeError(err)
}

func (c *Client) retry(ctx context.Context, call func(context.Context) error) error {
	var err error
	for attempt := range c.config.MaxAttempts {
		err = call(ctx)
		if err == nil || status.Code(err) != codes.Unavailable || attempt+1 == c.config.MaxAttempts {
			return err
		}
		timer := time.NewTimer(c.config.RetryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}
func (c *Client) cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), c.config.CleanupTimeout)
}
func encodeRef(ref domain.ReservationRef) *schedulerv1.ReservationRef {
	return &schedulerv1.ReservationRef{ReservationId: ref.ID(), Generation: ref.Generation(), Capability: ref.Capability(), SchemaVersion: schemaVersionV1}
}
func decodeRef(m *schedulerv1.ReservationRef) (domain.ReservationRef, error) {
	if m == nil || m.SchemaVersion != schemaVersionV1 {
		return domain.ReservationRef{}, errors.New("scheduler returned invalid reservation")
	}
	return domain.NewReservationRef(m.ReservationId, m.Generation, m.Capability)
}

func decodeError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return errors.New("scheduler request failed")
	}
	for _, raw := range st.Details() {
		detail, ok := raw.(*schedulerv1.ErrorDetail)
		if !ok {
			continue
		}
		if detail.SchemaVersion != schemaVersionV1 || detail.RetryAfterMs > uint32((30*time.Second)/time.Millisecond) {
			return errors.New("scheduler returned unsupported error detail")
		}
		switch detail.Code {
		case schedulerv1.ErrorCode_ERROR_CODE_IDEMPOTENCY_CONFLICT:
			return domain.ErrIdempotencyConflict
		case schedulerv1.ErrorCode_ERROR_CODE_REQUEST_IN_PROGRESS:
			return domain.ErrRequestInProgress
		case schedulerv1.ErrorCode_ERROR_CODE_REQUEST_NOT_REPLAYABLE:
			return domain.ErrRequestNotReplayable
		case schedulerv1.ErrorCode_ERROR_CODE_NO_CAPACITY:
			return domain.ErrNoCapacity
		case schedulerv1.ErrorCode_ERROR_CODE_INVALID_REFERENCE:
			return domain.ErrInvalidReference
		case schedulerv1.ErrorCode_ERROR_CODE_STALE_TARGET:
			return domain.ErrStaleTarget
		case schedulerv1.ErrorCode_ERROR_CODE_INVALID_STATE:
			return domain.ErrInvalidState
		case schedulerv1.ErrorCode_ERROR_CODE_CANCELED:
			return context.Canceled
		case schedulerv1.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED:
			return context.DeadlineExceeded
		case schedulerv1.ErrorCode_ERROR_CODE_UNAVAILABLE:
			return domain.NewPublicError(domain.ErrorUnavailable)
		case schedulerv1.ErrorCode_ERROR_CODE_INTERNAL:
			return errors.New("scheduler operation failed")
		default:
			return errors.New("scheduler returned unsupported error detail")
		}
	}
	switch st.Code() {
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
