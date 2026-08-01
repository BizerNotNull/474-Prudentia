package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/jackc/pgx/v5"
)

func (c *ControllerCatalog) ResolveCapacityDebt(ctx context.Context, cmd domain.DebtResolution) error {
	proof := cmd.Proof()
	identity := proof.Identity()
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin capacity debt resolution: %w", err)
	}
	defer tx.Rollback(ctx)

	var currentGeneration uint64
	err = tx.QueryRow(ctx, `SELECT current_generation FROM controller_writer_generations
		WHERE cluster=$1 FOR UPDATE`, identity.Cluster()).Scan(&currentGeneration)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && currentGeneration != uint64(proof.WriterGeneration())) {
		return domain.ErrStaleWriterGeneration
	}
	if err != nil {
		return fmt.Errorf("check debt resolver writer generation: %w", err)
	}

	debt, err := lockCapacityDebt(ctx, tx, cmd.DebtID())
	if err != nil {
		return err
	}
	if debt.reservationID != cmd.ReservationID() || !sameIdentity(debt.identity, identity) {
		return domain.ErrInvalidReference
	}
	evidenceHash := proof.EvidenceHash()
	if debt.state == "resolved_identity_gone" {
		if !equalBytes(debt.resolutionEvidenceHash, evidenceHash[:]) {
			return domain.ErrCapacityDebtConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit duplicate capacity debt resolution: %w", err)
		}
		return nil
	}
	if debt.state != "active" {
		return domain.ErrInvalidState
	}

	reservation, err := lockOrphanedDebtReservation(ctx, tx, debt.reservationID)
	if err != nil {
		return err
	}
	if reservation.state != "orphaned" || !equalBytes(reservation.tenantHash, debt.tenantHash) ||
		reservation.slotCost != debt.slotCost || !sameReservationIdentity(reservation, debt.identity) {
		return domain.ErrInvalidState
	}
	if err := transitionGrant(ctx, tx, reservation, "orphaned", "released", grantCounterResolveOrphan); err != nil {
		return err
	}
	if err := decrementBackendOrphanedCapacity(ctx, tx, reservation); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE capacity_debts
		SET state='resolved_identity_gone', resolution_evidence_hash=$2,
			resolved_at=clock_timestamp(), updated_at=clock_timestamp()
		WHERE debt_id=$1 AND state='active'`, cmd.DebtID(), evidenceHash[:])
	if err != nil {
		return fmt.Errorf("resolve capacity debt: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit capacity debt resolution: %w", err)
	}
	return nil
}

type capacityDebtRow struct {
	reservationID          string
	tenantHash             []byte
	identity               domain.WorkloadIdentity
	slotCost               int
	state                  string
	resolutionEvidenceHash []byte
}

func lockCapacityDebt(ctx context.Context, tx pgx.Tx, debtID string) (capacityDebtRow, error) {
	var (
		debt           capacityDebtRow
		identityParams domain.WorkloadIdentityParams
	)
	err := tx.QueryRow(ctx, `SELECT reservation_id, tenant_hash, cluster, namespace,
		logical_engine, pod_uid, endpoint_epoch, recovery_epoch, slot_cost, state,
		resolution_evidence_hash
		FROM capacity_debts WHERE debt_id=$1 FOR UPDATE`, debtID).
		Scan(&debt.reservationID, &debt.tenantHash, &identityParams.Cluster, &identityParams.Namespace,
			&identityParams.LogicalEngine, &identityParams.PodUID, &identityParams.EndpointEpoch,
			&identityParams.RecoveryEpoch, &debt.slotCost, &debt.state, &debt.resolutionEvidenceHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return capacityDebtRow{}, domain.ErrCapacityDebtNotFound
	}
	if err != nil {
		return capacityDebtRow{}, fmt.Errorf("lock capacity debt: %w", err)
	}
	debt.identity, err = domain.NewWorkloadIdentity(identityParams)
	if err != nil {
		return capacityDebtRow{}, domain.ErrInvalidState
	}
	return debt, nil
}

func lockOrphanedDebtReservation(ctx context.Context, tx pgx.Tx, reservationID string) (reservationRow, error) {
	var row reservationRow
	err := tx.QueryRow(ctx, `SELECT state, cluster, namespace, logical_engine, pod_uid,
		endpoint_epoch, recovery_epoch, slot_cost, tenant_hash
		FROM scheduler_reservations WHERE reservation_id=$1 FOR UPDATE`, reservationID).
		Scan(&row.state, &row.cluster, &row.namespace, &row.engine, &row.podUID,
			&row.endpointEpoch, &row.recoveryEpoch, &row.slotCost, &row.tenantHash)
	if err != nil {
		return reservationRow{}, domain.ErrInvalidState
	}
	row.reservationID = reservationID
	return row, nil
}

func decrementBackendOrphanedCapacity(ctx context.Context, tx pgx.Tx, row reservationRow) error {
	tag, err := tx.Exec(ctx, `UPDATE scheduler_backends
		SET orphaned_slots=orphaned_slots-$7
		WHERE cluster=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4
		  AND endpoint_epoch=$5 AND recovery_epoch=$6 AND orphaned_slots >= $7`,
		row.cluster, row.namespace, row.engine, row.podUID, row.endpointEpoch, row.recoveryEpoch, row.slotCost)
	if err != nil {
		return fmt.Errorf("resolve backend orphaned capacity: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	return nil
}

func sameIdentity(left, right domain.WorkloadIdentity) bool {
	return left.Cluster() == right.Cluster() && left.Namespace() == right.Namespace() &&
		left.LogicalEngine() == right.LogicalEngine() && left.PodUID() == right.PodUID() &&
		left.EndpointEpoch() == right.EndpointEpoch() && left.RecoveryEpoch() == right.RecoveryEpoch()
}

func sameReservationIdentity(row reservationRow, identity domain.WorkloadIdentity) bool {
	return row.cluster == identity.Cluster() && row.namespace == identity.Namespace() &&
		row.engine == identity.LogicalEngine() && row.podUID == identity.PodUID() &&
		row.endpointEpoch == identity.EndpointEpoch() && row.recoveryEpoch == identity.RecoveryEpoch()
}
