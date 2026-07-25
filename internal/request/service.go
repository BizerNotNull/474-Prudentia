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
	executionBudget time.Duration
	cleanupTimeout  time.Duration
}

func NewService(scheduler Scheduler, provider Provider, executionBudget, cleanupTimeout time.Duration) (*Service, error) {
	if scheduler == nil || provider == nil || executionBudget <= 0 || executionBudget > 30*time.Minute || cleanupTimeout <= 0 || cleanupTimeout > 30*time.Second {
		return nil, errors.New("invalid inference service configuration")
	}
	return &Service{scheduler: scheduler, provider: provider, executionBudget: executionBudget, cleanupTimeout: cleanupTimeout}, nil
}

func (s *Service) Infer(ctx context.Context, request domain.AuthorizedRequest, _ domain.ResponseMode, sink publichttp.StreamSink) error {
	requestID, err := randomID("req_")
	if err != nil {
		return domain.NewPublicError(domain.ErrorInternal)
	}
	attemptID, err := randomID("att_")
	if err != nil {
		return domain.NewPublicError(domain.ErrorInternal)
	}
	command, err := domain.NewScheduleCommand(domain.ScheduleParams{
		RequestID: requestID, AttemptID: attemptID, Tenant: request.Tenant(), Model: request.Request().Model(),
		SlotCost: 1, ExecutionBudget: s.executionBudget,
	})
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
