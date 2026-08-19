package functional_test

import (
	"context"
	"errors"
	"testing"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/BizerNotNull/474-Prudentia/internal/scheduling"
)

type transitionStore struct {
	state  string
	proof  domain.TerminalProof
	cause  domain.AmbiguousCause
	reason domain.GiveUpReason
}

func (*transitionStore) Candidates(context.Context, domain.ScheduleCommand) (domain.CandidateCatalog, error) {
	return domain.CandidateCatalog{}, domain.ErrNoCapacity
}
func (*transitionStore) LookupReservation(context.Context, domain.ScheduleCommand) (domain.Reservation, bool, error) {
	return domain.Reservation{}, false, nil
}
func (*transitionStore) TryReserve(context.Context, domain.ScheduleCommand, domain.WorkloadIdentity) (domain.Reservation, error) {
	return domain.Reservation{}, domain.ErrNoCapacity
}
func (*transitionStore) PrepareDispatch(context.Context, domain.ReservationRef) (domain.DispatchTarget, error) {
	return domain.DispatchTarget{}, domain.ErrInvalidState
}
func (s *transitionStore) AbandonNeverDispatched(_ context.Context, _ domain.ReservationRef, reason domain.RerankReason) error {
	if s.state != "reserved" || reason != domain.RerankStaleTarget {
		return domain.ErrInvalidState
	}
	s.state = "retained_rerank"
	return nil
}
func (s *transitionStore) GiveUpNeverDispatched(_ context.Context, _ domain.ReservationRef, reason domain.GiveUpReason) error {
	if s.state != "reserved" && s.state != "retained_rerank" {
		return domain.ErrInvalidState
	}
	if s.state == "given_up" && s.reason != reason {
		return domain.ErrInvalidState
	}
	s.state, s.reason = "given_up", reason
	return nil
}
func (s *transitionStore) ReleaseTerminal(_ context.Context, _ domain.ReservationRef, proof domain.TerminalProof) error {
	if proof == domain.TerminalProofAuthenticatedProviderTermination {
		return domain.ErrInvalidState
	}
	if s.state == "released" {
		if s.proof == proof {
			return nil
		}
		return domain.ErrInvalidState
	}
	if s.state != "dispatch_authorized" {
		return domain.ErrInvalidState
	}
	s.state, s.proof = "released", proof
	return nil
}
func (s *transitionStore) ConvertToOrphanDebt(_ context.Context, _ domain.ReservationRef, cause domain.AmbiguousCause) error {
	if s.state == "orphaned" {
		if s.cause == cause {
			return nil
		}
		return domain.ErrInvalidState
	}
	if s.state != "dispatch_authorized" {
		return domain.ErrInvalidState
	}
	s.state, s.cause = "orphaned", cause
	return nil
}

func transitionService(t *testing.T, state string) (*scheduling.Service, *transitionStore, domain.ReservationRef) {
	t.Helper()
	store := &transitionStore{state: state}
	service, err := scheduling.NewService(store, 2)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := domain.NewReservationRef("reservation-1", 1, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	return service, store, ref
}

func TestSchedulerTerminalEvidenceTransitionTable(t *testing.T) {
	for name, proof := range map[string]domain.TerminalProof{
		"stream protocol finish":   domain.TerminalProofProviderFinish,
		"bounded response and EOF": domain.TerminalProofCompleteNonStreaming,
		"body proven not sent":     domain.TerminalProofNotSent,
	} {
		t.Run(name, func(t *testing.T) {
			service, store, ref := transitionService(t, "dispatch_authorized")
			if err := service.Finalize(context.Background(), ref, proof); err != nil {
				t.Fatal(err)
			}
			if store.state != "released" || store.proof != proof {
				t.Fatalf("state/proof = %q/%d", store.state, store.proof)
			}
			if err := service.Finalize(context.Background(), ref, proof); err != nil {
				t.Fatalf("identical terminal retry: %v", err)
			}
			other := domain.TerminalProofProviderFinish
			if proof == other {
				other = domain.TerminalProofNotSent
			}
			if err := service.Finalize(context.Background(), ref, other); !errors.Is(err, domain.ErrInvalidState) {
				t.Fatalf("conflicting terminal retry = %v", err)
			}
		})
	}
	t.Run("termination enum alone is not authenticated evidence", func(t *testing.T) {
		service, store, ref := transitionService(t, "dispatch_authorized")
		if err := service.Finalize(context.Background(), ref, domain.TerminalProofAuthenticatedProviderTermination); !errors.Is(err, domain.ErrInvalidState) {
			t.Fatalf("error = %v", err)
		}
		if store.state != "dispatch_authorized" {
			t.Fatalf("state = %q", store.state)
		}
	})
}

func TestSchedulerPreDispatchAndAmbiguityTransitionTable(t *testing.T) {
	t.Run("abandon retains grant", func(t *testing.T) {
		service, store, ref := transitionService(t, "reserved")
		if err := service.AbandonBeforeDispatch(context.Background(), ref, domain.RerankStaleTarget); err != nil {
			t.Fatal(err)
		}
		if store.state != "retained_rerank" {
			t.Fatalf("state = %q", store.state)
		}
		if err := service.GiveUpBeforeDispatch(context.Background(), ref, domain.GiveUpReranksExhausted); err != nil {
			t.Fatal(err)
		}
		if store.state != "given_up" {
			t.Fatalf("state = %q", store.state)
		}
	})
	for name, cause := range map[string]domain.AmbiguousCause{
		"transport reset":     domain.AmbiguousTransport,
		"client cancellation": domain.AmbiguousCanceled,
		"protocol failure":    domain.AmbiguousProtocol,
	} {
		t.Run(name, func(t *testing.T) {
			service, store, ref := transitionService(t, "dispatch_authorized")
			if err := service.MarkAmbiguous(context.Background(), ref, cause); err != nil {
				t.Fatal(err)
			}
			if store.state != "orphaned" || store.cause != cause {
				t.Fatalf("state/cause = %q/%d", store.state, store.cause)
			}
			if err := service.MarkAmbiguous(context.Background(), ref, cause); err != nil {
				t.Fatalf("identical debt retry: %v", err)
			}
		})
	}
}
