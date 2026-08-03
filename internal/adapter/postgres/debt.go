package postgres

import (
	"context"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

// ResolveCapacityDebt keeps the controller facet on the same authoritative
// orphaned_capacity_debts ledger as the scheduler ambiguity path.
func (c *ControllerCatalog) ResolveCapacityDebt(ctx context.Context, cmd domain.DebtResolution) error {
	return c.Catalog.ResolveCapacityDebt(ctx, cmd)
}
