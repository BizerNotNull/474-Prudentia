package schedulergrpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	schedulerv1 "github.com/BizerNotNull/474-Prudentia/api/scheduler/v1"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	SchemaVersionV1     uint32 = 1
	MaxRequestIDBytes          = 128
	MaxAttemptIDBytes          = 128
	MaxTenantScopeBytes        = 256
	MaxModelBytes              = 256
	MaxEndpointBytes           = 2048
	MaxRetryAfter              = 30 * time.Second
)

type Codec struct{}

func (Codec) DecodeSchedule(m *schedulerv1.ScheduleRequest) (domain.ScheduleCommand, error) {
	if m == nil || m.SchemaVersion != SchemaVersionV1 || m.BudgetSchemaVersion != SchemaVersionV1 ||
		len(m.RequestId) == 0 || len(m.RequestId) > MaxRequestIDBytes || len(m.AttemptId) == 0 || len(m.AttemptId) > MaxAttemptIDBytes ||
		len(m.TenantScope) == 0 || len(m.TenantScope) > MaxTenantScopeBytes || len(m.Model) == 0 || len(m.Model) > MaxModelBytes ||
		len(m.IdempotencyLookupCandidates) > domain.MaxLookupCandidates || len(m.DigestCandidates) == 0 || len(m.DigestCandidates) > domain.MaxDigestCandidates ||
		m.ExecutionBudgetMs <= 0 || m.ExecutionBudgetMs > int64((30*time.Minute)/time.Millisecond) || m.Priority != schedulerv1.Priority_PRIORITY_NORMAL ||
		m.Features == nil || m.Features.SchemaVersion != SchemaVersionV1 {
		return domain.ScheduleCommand{}, errors.New("invalid schedule request")
	}
	features, err := domain.NewFeatureSet(domain.FeatureVersion(m.Features.SchemaVersion), m.Features.Bits)
	if err != nil {
		return domain.ScheduleCommand{}, errors.New("invalid schedule request")
	}
	lookups := make([]domain.IdempotencyLookupCandidate, len(m.IdempotencyLookupCandidates))
	for i, candidate := range m.IdempotencyLookupCandidates {
		if candidate == nil {
			return domain.ScheduleCommand{}, errors.New("invalid schedule request")
		}
		lookups[i], err = domain.NewIdempotencyLookupCandidate(candidate.PepperVersion, candidate.HmacSha256)
		if err != nil {
			return domain.ScheduleCommand{}, errors.New("invalid schedule request")
		}
	}
	digests := make([]domain.RequestDigestCandidate, len(m.DigestCandidates))
	for i, candidate := range m.DigestCandidates {
		if candidate == nil {
			return domain.ScheduleCommand{}, errors.New("invalid schedule request")
		}
		digests[i], err = domain.NewRequestDigestCandidate(candidate.DigestVersion, candidate.HmacSha256)
		if err != nil {
			return domain.ScheduleCommand{}, errors.New("invalid schedule request")
		}
	}
	cmd, err := domain.NewScheduleCommand(domain.ScheduleParams{RequestID: m.RequestId, AttemptID: m.AttemptId, Tenant: string(m.TenantScope), IdempotencyCandidates: lookups, LookupWriteVersion: m.LookupWriteVersion, DigestCandidates: digests, DigestWriteVersion: m.DigestWriteVersion, Model: m.Model, SlotCost: m.SlotCost, Features: features, ExecutionBudget: time.Duration(m.ExecutionBudgetMs) * time.Millisecond})
	if err != nil {
		return domain.ScheduleCommand{}, errors.New("invalid schedule request")
	}
	return cmd, nil
}

func (Codec) DecodeReservationRef(m *schedulerv1.ReservationRef) (domain.ReservationRef, error) {
	if m == nil || m.SchemaVersion != SchemaVersionV1 || len(m.ReservationId) > MaxRequestIDBytes || len(m.Capability) > domain.MaxCapabilityBytes {
		return domain.ReservationRef{}, domain.ErrInvalidReference
	}
	return domain.NewReservationRef(m.ReservationId, m.Generation, m.Capability)
}

func (Codec) EncodeReservation(v domain.Reservation) (*schedulerv1.Reservation, error) {
	ref := v.Ref()
	if _, err := domain.NewReservationRef(ref.ID(), ref.Generation(), ref.Capability()); err != nil {
		return nil, err
	}
	return &schedulerv1.Reservation{Ref: encodeReservationRef(ref), SchemaVersion: SchemaVersionV1}, nil
}

func encodeReservationRef(ref domain.ReservationRef) *schedulerv1.ReservationRef {
	return &schedulerv1.ReservationRef{ReservationId: ref.ID(), Generation: ref.Generation(), Capability: ref.Capability(), SchemaVersion: SchemaVersionV1}
}

func (Codec) EncodeError(err error) (codes.Code, *schedulerv1.ErrorDetail) {
	code, detailCode := codes.Internal, schedulerv1.ErrorCode_ERROR_CODE_INTERNAL
	switch {
	case errors.Is(err, domain.ErrIdempotencyConflict):
		code, detailCode = codes.AlreadyExists, schedulerv1.ErrorCode_ERROR_CODE_IDEMPOTENCY_CONFLICT
	case errors.Is(err, domain.ErrRequestInProgress):
		code, detailCode = codes.Aborted, schedulerv1.ErrorCode_ERROR_CODE_REQUEST_IN_PROGRESS
	case errors.Is(err, domain.ErrRequestNotReplayable):
		code, detailCode = codes.NotFound, schedulerv1.ErrorCode_ERROR_CODE_REQUEST_NOT_REPLAYABLE
	case errors.Is(err, domain.ErrNoCapacity):
		code, detailCode = codes.ResourceExhausted, schedulerv1.ErrorCode_ERROR_CODE_NO_CAPACITY
	case errors.Is(err, domain.ErrInvalidReference):
		code, detailCode = codes.PermissionDenied, schedulerv1.ErrorCode_ERROR_CODE_INVALID_REFERENCE
	case errors.Is(err, domain.ErrStaleTarget):
		code, detailCode = codes.OutOfRange, schedulerv1.ErrorCode_ERROR_CODE_STALE_TARGET
	case errors.Is(err, domain.ErrInvalidState):
		code, detailCode = codes.FailedPrecondition, schedulerv1.ErrorCode_ERROR_CODE_INVALID_STATE
	case errors.Is(err, context.Canceled):
		code, detailCode = codes.Canceled, schedulerv1.ErrorCode_ERROR_CODE_CANCELED
	case errors.Is(err, context.DeadlineExceeded):
		code, detailCode = codes.DeadlineExceeded, schedulerv1.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED
	}
	return code, &schedulerv1.ErrorDetail{SchemaVersion: SchemaVersionV1, Code: detailCode}
}

func transportError(codec Codec, err error) error {
	code, detail := codec.EncodeError(err)
	st := status.New(code, safeMessage(code))
	withDetail, attachErr := st.WithDetails(detail)
	if attachErr != nil {
		return status.Error(codes.Internal, "scheduler operation failed")
	}
	return withDetail.Err()
}

func invalidArgument(codec Codec) error {
	st := status.New(codes.InvalidArgument, "invalid request")
	withDetail, err := st.WithDetails(&schedulerv1.ErrorDetail{SchemaVersion: SchemaVersionV1, Code: schedulerv1.ErrorCode_ERROR_CODE_INVALID_REQUEST})
	if err != nil {
		return st.Err()
	}
	return withDetail.Err()
}

func safeMessage(code codes.Code) string {
	switch code {
	case codes.AlreadyExists:
		return "idempotency conflict"
	case codes.Aborted:
		return "request in progress"
	case codes.NotFound:
		return "request not replayable"
	case codes.ResourceExhausted:
		return "capacity unavailable"
	case codes.PermissionDenied:
		return "reservation denied"
	case codes.OutOfRange:
		return "target stale"
	case codes.FailedPrecondition:
		return "invalid state"
	case codes.Canceled:
		return "request canceled"
	case codes.DeadlineExceeded:
		return "deadline exceeded"
	default:
		return "scheduler operation failed"
	}
}

func requireV1(version uint32) error {
	if version != SchemaVersionV1 {
		return fmt.Errorf("unsupported schema version")
	}
	return nil
}
