package postgres

import (
	"context"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

func (c *Catalog) Candidates(ctx context.Context, cmd domain.ScheduleCommand) (domain.CandidateCatalog, error) {
	return c.SchedulerStore().Candidates(ctx, cmd)
}
func (c *Catalog) LookupReservation(ctx context.Context, cmd domain.ScheduleCommand) (domain.Reservation, bool, error) {
	return c.SchedulerStore().LookupReservation(ctx, cmd)
}
func (c *Catalog) TryReserve(ctx context.Context, cmd domain.ScheduleCommand, id domain.WorkloadIdentity) (domain.Reservation, error) {
	return c.SchedulerStore().TryReserve(ctx, cmd, id)
}
func (c *Catalog) PrepareDispatch(ctx context.Context, ref domain.ReservationRef) (domain.DispatchTarget, error) {
	return c.SchedulerStore().PrepareDispatch(ctx, ref)
}
func (c *Catalog) AbandonNeverDispatched(ctx context.Context, ref domain.ReservationRef, reason domain.RerankReason) error {
	return c.SchedulerStore().AbandonBeforeDispatch(ctx, ref, reason)
}
func (c *Catalog) GiveUpNeverDispatched(ctx context.Context, ref domain.ReservationRef, reason domain.GiveUpReason) error {
	return c.SchedulerStore().GiveUpBeforeDispatch(ctx, ref, reason)
}
func (c *Catalog) ReleaseTerminal(ctx context.Context, ref domain.ReservationRef, proof domain.TerminalProof) error {
	return c.SchedulerStore().Finalize(ctx, ref, proof)
}
func (c *Catalog) ConvertToOrphanDebt(ctx context.Context, ref domain.ReservationRef, cause domain.AmbiguousCause) error {
	return c.SchedulerStore().MarkAmbiguous(ctx, ref, cause)
}

func (s *SchedulerStore) AbandonNeverDispatched(ctx context.Context, ref domain.ReservationRef, reason domain.RerankReason) error {
	return s.AbandonBeforeDispatch(ctx, ref, reason)
}
func (s *SchedulerStore) GiveUpNeverDispatched(ctx context.Context, ref domain.ReservationRef, reason domain.GiveUpReason) error {
	return s.GiveUpBeforeDispatch(ctx, ref, reason)
}
func (s *SchedulerStore) ReleaseTerminal(ctx context.Context, ref domain.ReservationRef, proof domain.TerminalProof) error {
	return s.Finalize(ctx, ref, proof)
}
func (s *SchedulerStore) ConvertToOrphanDebt(ctx context.Context, ref domain.ReservationRef, cause domain.AmbiguousCause) error {
	return s.MarkAmbiguous(ctx, ref, cause)
}
