package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/jackc/pgx/v5"
)

type authoritativeDebt struct {
	reservationID, requestID, state string
	tenantHash                      []byte
	identity                        domain.WorkloadIdentity
	slotCost                        int
	evidenceType                    string
	evidenceHash                    []byte
}

func readDebtIdentity(ctx context.Context, tx pgx.Tx, debtID string) (authoritativeDebt, error) {
	var d authoritativeDebt
	var p domain.WorkloadIdentityParams
	err := tx.QueryRow(ctx, `SELECT reservation_id,request_id,tenant_hash,cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,slot_cost,state,COALESCE(resolution_evidence_type,''),resolution_evidence_hash FROM orphaned_capacity_debts WHERE debt_id=$1`, debtID).Scan(&d.reservationID, &d.requestID, &d.tenantHash, &p.Cluster, &p.Namespace, &p.LogicalEngine, &p.PodUID, &p.EndpointEpoch, &p.RecoveryEpoch, &d.slotCost, &d.state, &d.evidenceType, &d.evidenceHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, domain.ErrCapacityDebtNotFound
	}
	if err != nil {
		return d, fmt.Errorf("read capacity debt: %w", err)
	}
	d.identity, err = domain.NewWorkloadIdentity(p)
	if err != nil {
		return d, domain.ErrInvalidState
	}
	return d, nil
}

func lockDebtAccounting(ctx context.Context, tx pgx.Tx, debtID string, expected authoritativeDebt) (authoritativeDebt, error) {
	// Preserve the global subset order: system -> tenant/grant -> exact capacity -> reservation/debt.
	var ignored string
	if err := tx.QueryRow(ctx, `SELECT admission_state FROM system_admission_state WHERE cluster_id=$1 FOR UPDATE`, expected.identity.Cluster()).Scan(&ignored); err != nil {
		return authoritativeDebt{}, err
	}
	var orphaned int
	if err := tx.QueryRow(ctx, `SELECT orphaned_grants FROM tenant_counters WHERE tenant_hash=$1 FOR UPDATE`, expected.tenantHash).Scan(&orphaned); err != nil {
		return authoritativeDebt{}, err
	}
	var slots int
	if err := tx.QueryRow(ctx, `SELECT orphaned_slots FROM instance_capacity WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 FOR UPDATE`, identityArgs(expected.identity)...).Scan(&slots); err != nil {
		return authoritativeDebt{}, err
	}
	var reservationState string
	if err := tx.QueryRow(ctx, `SELECT state FROM reservations WHERE reservation_id=$1 FOR UPDATE`, expected.reservationID).Scan(&reservationState); err != nil {
		return authoritativeDebt{}, err
	}
	if reservationState != "orphaned" {
		return authoritativeDebt{}, domain.ErrInvalidState
	}
	current, err := readDebtIdentity(ctx, tx, debtID)
	if err != nil {
		return authoritativeDebt{}, err
	}
	if current.reservationID != expected.reservationID || !current.identity.Equal(expected.identity) || current.slotCost != expected.slotCost {
		return authoritativeDebt{}, domain.ErrCapacityDebtConflict
	}
	if current.state == "active" && (orphaned < current.slotCost || slots < current.slotCost) {
		return authoritativeDebt{}, domain.ErrInvalidState
	}
	return current, nil
}

func resolveDebtCounters(ctx context.Context, tx pgx.Tx, d authoritativeDebt) error {
	if tag, err := tx.Exec(ctx, `UPDATE admission_grants SET state='released',updated_at=transaction_timestamp() WHERE request_id=$1 AND state='orphaned'`, d.requestID); err != nil {
		return err
	} else if tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	if tag, err := tx.Exec(ctx, `UPDATE tenant_counters SET orphaned_grants=orphaned_grants-$2,version=version+1 WHERE tenant_hash=$1 AND orphaned_grants >= $2`, d.tenantHash, d.slotCost); err != nil {
		return err
	} else if tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	args := append(identityArgs(d.identity), d.slotCost)
	if tag, err := tx.Exec(ctx, `UPDATE instance_capacity SET orphaned_slots=orphaned_slots-$7,updated_at=transaction_timestamp() WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 AND orphaned_slots >= $7`, args...); err != nil {
		return err
	} else if tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	return nil
}

func (c *Catalog) ResolveCapacityDebt(ctx context.Context, cmd domain.DebtResolution) error {
	var identity domain.WorkloadIdentity
	var evidence [sha256.Size]byte
	var writer domain.WriterGeneration
	var actor [sha256.Size]byte
	switch cmd.Kind() {
	case domain.DebtResolutionIdentityGone:
		proof, ok := cmd.IdentityGoneProof()
		if !ok {
			return domain.ErrInvalidDebtEvidence
		}
		identity = proof.Identity()
		evidence = proof.EvidenceHash()
		writer = proof.WriterGeneration()
		actor = sha256.Sum256([]byte(fmt.Sprint(writer)))
	case domain.DebtResolutionProviderTermination:
		proof, ok := cmd.ProviderTerminationProof()
		if !ok {
			return domain.ErrInvalidDebtEvidence
		}
		identity = proof.Identity()
		evidence = proof.EvidenceHash()
		actor = sha256.Sum256([]byte(proof.ManifestID()))
	default:
		return domain.ErrInvalidDebtEvidence
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	initial, err := readDebtIdentity(ctx, tx, cmd.DebtID())
	if err != nil {
		return err
	}
	if initial.reservationID != cmd.ReservationID() || !initial.identity.Equal(identity) {
		return domain.ErrCapacityDebtConflict
	}
	if writer != 0 {
		if err := lockWriter(ctx, tx, identity.Cluster(), writer); err != nil {
			return err
		}
	}
	current, err := lockDebtAccounting(ctx, tx, cmd.DebtID(), initial)
	if err != nil {
		return err
	}
	kind := cmd.Kind().String()
	resolvedState := "resolved_" + kind
	if current.state == resolvedState && current.evidenceType == kind && equalBytes(current.evidenceHash, evidence[:]) {
		return tx.Commit(ctx)
	}
	if current.state != "active" {
		return domain.ErrCapacityDebtConflict
	}
	if err := resolveDebtCounters(ctx, tx, current); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE orphaned_capacity_debts SET state=$2,resolution_evidence_type=$3,resolution_evidence_hash=$4,resolution_actor_hash=$5,resolved_at=transaction_timestamp(),updated_at=transaction_timestamp() WHERE debt_id=$1 AND state='active'`, cmd.DebtID(), resolvedState, kind, evidence[:], actor[:])
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrCapacityDebtConflict
	}
	return tx.Commit(ctx)
}

func (c *Catalog) UnsafeOverrideCapacityDebt(ctx context.Context, cmd domain.UnsafeDebtOverride) error {
	if cmd.Confirmation() != domain.UnsafeDebtOverrideDangerPhrase {
		return domain.ErrInvalidUnsafeDebtOverride
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	initial, err := readDebtIdentity(ctx, tx, cmd.DebtID())
	if err != nil {
		return err
	}
	if !initial.identity.Equal(cmd.ExpectedIdentity()) {
		return domain.ErrCapacityDebtConflict
	}
	current, err := lockDebtAccounting(ctx, tx, cmd.DebtID(), initial)
	if err != nil {
		return err
	}
	principal := cmd.Principal().IdentityHash()
	evidence := sha256.Sum256([]byte(cmd.DebtID() + "\x00" + cmd.Ticket() + "\x00" + cmd.Reason()))
	if current.state == "unsafe_overridden" && current.evidenceType == "unsafe_override" && equalBytes(current.evidenceHash, evidence[:]) {
		return tx.Commit(ctx)
	}
	if current.state != "active" {
		return domain.ErrCapacityDebtConflict
	}
	if err := resolveDebtCounters(ctx, tx, current); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE orphaned_capacity_debts SET state='unsafe_overridden',resolution_evidence_type='unsafe_override',resolution_evidence_hash=$2,resolution_actor_hash=$3,override_ticket=$4,override_reason=$5,resolved_at=transaction_timestamp(),updated_at=transaction_timestamp() WHERE debt_id=$1 AND state='active'`, cmd.DebtID(), evidence[:], principal[:], cmd.Ticket(), cmd.Reason())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrCapacityDebtConflict
	}
	eventID, err := randomID("audit_")
	if err != nil {
		return err
	}
	target := sha256.Sum256([]byte(cmd.DebtID()))
	_, err = tx.Exec(ctx, `INSERT INTO audit_events(event_id,event_type,actor_identity_hash,service_identity_hash,target_type,target_hash,debt_id,reason,ticket,event_metadata) VALUES($1,'debt_unsafe_overridden',$2,$2,'capacity_debt',$3,$4,$5,$6,jsonb_build_object('identity_hash',$7))`, eventID, principal[:], target[:], cmd.DebtID(), cmd.Reason(), cmd.Ticket(), sha256Bytes(fmt.Sprint(cmd.ExpectedIdentity())))
	if err != nil {
		return fmt.Errorf("append unsafe override audit: %w", err)
	}
	return tx.Commit(ctx)
}
