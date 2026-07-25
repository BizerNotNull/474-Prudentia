package scheduling

import (
	"context"
	"errors"
	"sort"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type Candidate struct {
	Identity       domain.WorkloadIdentity
	AvailableSlots uint32
}

type Store interface {
	Candidates(context.Context, domain.ScheduleCommand) ([]Candidate, error)
	TryReserve(context.Context, domain.ScheduleCommand, domain.WorkloadIdentity) (domain.Reservation, error)
	PrepareDispatch(context.Context, domain.ReservationRef) (domain.DispatchTarget, error)
	GiveUpBeforeDispatch(context.Context, domain.ReservationRef, domain.GiveUpReason) error
	Finalize(context.Context, domain.ReservationRef, domain.TerminalProof) error
	MarkAmbiguous(context.Context, domain.ReservationRef, domain.AmbiguousCause) error
}

type Service struct {
	store         Store
	maxRerankRead int
}

func NewService(store Store, maxRerankRead int) (*Service, error) {
	if store == nil || maxRerankRead < 1 || maxRerankRead > 8 {
		return nil, errors.New("invalid scheduling service configuration")
	}
	return &Service{store: store, maxRerankRead: maxRerankRead}, nil
}

func Rank(candidates []Candidate, slotCost uint32) []Candidate {
	ranked := make([]Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.AvailableSlots >= slotCost {
			ranked = append(ranked, candidate)
		}
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].AvailableSlots != ranked[j].AvailableSlots {
			return ranked[i].AvailableSlots > ranked[j].AvailableSlots
		}
		left, right := ranked[i].Identity, ranked[j].Identity
		if left.Cluster() != right.Cluster() {
			return left.Cluster() < right.Cluster()
		}
		if left.Namespace() != right.Namespace() {
			return left.Namespace() < right.Namespace()
		}
		if left.LogicalEngine() != right.LogicalEngine() {
			return left.LogicalEngine() < right.LogicalEngine()
		}
		if left.PodUID() != right.PodUID() {
			return left.PodUID() < right.PodUID()
		}
		return left.EndpointEpoch() < right.EndpointEpoch()
	})
	return ranked
}

func (s *Service) Schedule(ctx context.Context, cmd domain.ScheduleCommand) (domain.Reservation, error) {
	for range s.maxRerankRead {
		candidates, err := s.store.Candidates(ctx, cmd)
		if err != nil {
			return domain.Reservation{}, err
		}
		for _, candidate := range Rank(candidates, cmd.SlotCost()) {
			reservation, err := s.store.TryReserve(ctx, cmd, candidate.Identity)
			if err == nil {
				return reservation, nil
			}
			if !errors.Is(err, domain.ErrNoCapacity) && !errors.Is(err, domain.ErrStaleTarget) {
				return domain.Reservation{}, err
			}
		}
	}
	return domain.Reservation{}, domain.ErrNoCapacity
}

func (s *Service) PrepareDispatch(ctx context.Context, ref domain.ReservationRef) (domain.DispatchTarget, error) {
	return s.store.PrepareDispatch(ctx, ref)
}

func (s *Service) GiveUpBeforeDispatch(ctx context.Context, ref domain.ReservationRef, reason domain.GiveUpReason) error {
	return s.store.GiveUpBeforeDispatch(ctx, ref, reason)
}

func (s *Service) Finalize(ctx context.Context, ref domain.ReservationRef, proof domain.TerminalProof) error {
	return s.store.Finalize(ctx, ref, proof)
}

func (s *Service) MarkAmbiguous(ctx context.Context, ref domain.ReservationRef, cause domain.AmbiguousCause) error {
	return s.store.MarkAmbiguous(ctx, ref, cause)
}
