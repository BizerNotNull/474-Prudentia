package request

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/BizerNotNull/474-Prudentia/internal/transport/publichttp"
)

type Scheduler interface {
	Schedule(context.Context, domain.ScheduleCommand) (domain.Reservation, error)
	PrepareDispatch(context.Context, domain.ReservationRef) (domain.DispatchTarget, error)
	GiveUpBeforeDispatch(context.Context, domain.ReservationRef, domain.GiveUpReason) error
	Finalize(context.Context, domain.ReservationRef, domain.TerminalProof) error
	MarkAmbiguous(context.Context, domain.ReservationRef, domain.AmbiguousCause) error
}

type Provider interface {
	Infer(context.Context, domain.DispatchTarget, domain.AuthorizedRequest, publichttp.StreamSink) error
}

type DispatchError struct {
	Cause   error
	NotSent bool
}

func (e *DispatchError) Error() string { return "provider dispatch failed" }
func (e *DispatchError) Unwrap() error { return e.Cause }

func NewNotSentError(cause error) error { return &DispatchError{Cause: cause, NotSent: true} }

type Service struct {
	scheduler       Scheduler
	provider        Provider
	idempotency     idempotencyDeriver
	executionBudget time.Duration
	cleanupTimeout  time.Duration
}

func NewService(scheduler Scheduler, provider Provider, idempotencyConfig IdempotencyConfig, executionBudget, cleanupTimeout time.Duration) (*Service, error) {
	if scheduler == nil || provider == nil || executionBudget <= 0 || executionBudget > 30*time.Minute || cleanupTimeout <= 0 || cleanupTimeout > 30*time.Second {
		return nil, errors.New("invalid inference service configuration")
	}
	idempotency, err := newIdempotencyDeriver(idempotencyConfig)
	if err != nil {
		return nil, err
	}
	return &Service{scheduler: scheduler, provider: provider, idempotency: idempotency, executionBudget: executionBudget, cleanupTimeout: cleanupTimeout}, nil
}

func (s *Service) Infer(ctx context.Context, requestID string, idempotencyKey []byte, request domain.AuthorizedRequest, _ domain.ResponseMode, sink publichttp.StreamSink) error {
	lookupCandidates, digestCandidates, err := s.idempotency.derive(request, idempotencyKey)
	if err != nil {
		return err
	}
	attemptID, err := randomID("att_")
	if err != nil {
		return domain.NewPublicError(domain.ErrorInternal)
	}
	params := domain.ScheduleParams{
		RequestID: requestID, AttemptID: attemptID, Tenant: request.Tenant(), Model: request.Request().Model(),
		SlotCost: 1, ExecutionBudget: s.executionBudget,
	}
	if len(lookupCandidates) != 0 {
		params.IdempotencyCandidates = lookupCandidates
		params.LookupWriteVersion = s.idempotency.lookupWriteVersion
		params.DigestCandidates = digestCandidates
		params.DigestWriteVersion = s.idempotency.digestWriteVersion
	}
	command, err := domain.NewScheduleCommand(params)
	if err != nil {
		return domain.NewPublicError(domain.ErrorInternal)
	}

	reservation, err := s.scheduler.Schedule(ctx, command)
	if err != nil {
		return publicSchedulingError(err)
	}
	ref := reservation.Ref()
	target, err := s.scheduler.PrepareDispatch(ctx, ref)
	if err != nil {
		s.cleanupGiveUp(ref, giveUpReason(ctx, err))
		return publicSchedulingError(err)
	}

	if err := s.provider.Infer(ctx, target, request, sink); err != nil {
		var dispatchErr *DispatchError
		if errors.As(err, &dispatchErr) && dispatchErr.NotSent {
			s.cleanupFinalize(ref, domain.TerminalProofNotSent)
		} else {
			s.cleanupAmbiguous(ref, ambiguousCause(ctx, err))
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return domain.NewPublicError(domain.ErrorUnavailable)
	}
	if err := s.cleanupFinalize(ref, domain.TerminalProofProviderFinish); err != nil {
		return domain.NewPublicError(domain.ErrorInternal)
	}
	return nil
}

func (s *Service) cleanupGiveUp(ref domain.ReservationRef, reason domain.GiveUpReason) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cleanupTimeout)
	defer cancel()
	_ = s.scheduler.GiveUpBeforeDispatch(ctx, ref, reason)
}

func (s *Service) cleanupFinalize(ref domain.ReservationRef, proof domain.TerminalProof) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.cleanupTimeout)
	defer cancel()
	return s.scheduler.Finalize(ctx, ref, proof)
}

func (s *Service) cleanupAmbiguous(ref domain.ReservationRef, cause domain.AmbiguousCause) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cleanupTimeout)
	defer cancel()
	_ = s.scheduler.MarkAmbiguous(ctx, ref, cause)
}

func publicSchedulingError(err error) error {
	switch {
	case errors.Is(err, domain.ErrIdempotencyConflict):
		return domain.NewPublicError(domain.ErrorIdempotencyConflict)
	case errors.Is(err, domain.ErrRequestInProgress):
		return domain.NewPublicError(domain.ErrorRequestInProgress)
	case errors.Is(err, domain.ErrRequestNotReplayable):
		return domain.NewPublicError(domain.ErrorRequestNotReplayable)
	}
	if errors.Is(err, domain.ErrNoCapacity) || errors.Is(err, domain.ErrStaleTarget) || errors.Is(err, domain.ErrInvalidState) {
		return domain.NewPublicError(domain.ErrorUnavailable)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return domain.NewPublicError(domain.ErrorInternal)
}

func giveUpReason(ctx context.Context, err error) domain.GiveUpReason {
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return domain.GiveUpCanceled
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return domain.GiveUpBudgetExpired
	}
	return domain.GiveUpReranksExhausted
}

func ambiguousCause(ctx context.Context, err error) domain.AmbiguousCause {
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return domain.AmbiguousCanceled
	}
	return domain.AmbiguousTransport
}

func randomID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(raw[:]), nil
}
