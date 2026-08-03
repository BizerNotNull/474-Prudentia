package postgres

import (
	"context"
	"fmt"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/jackc/pgx/v5"
)

const MaxSweepBatch = 1000

type SweepResult struct{ GivenUp, ConvertedToDebt uint32 }

// SweepReservationStates performs only the two architecture-approved timed
// transitions. It never releases possibly-dispatched work and never clears a debt.
func (c *Catalog) SweepReservationStates(ctx context.Context, limit int) (SweepResult, error) {
	if limit < 1 || limit > MaxSweepBatch {
		return SweepResult{}, domain.ErrInvalidState
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return SweepResult{}, err
	}
	defer tx.Rollback(ctx)
	// The system row is always first, even though classification is allowed while fenced.
	if _, err := tx.Exec(ctx, `SELECT cluster_id FROM system_admission_state ORDER BY cluster_id FOR UPDATE`); err != nil {
		return SweepResult{}, err
	}
	rows, err := tx.Query(ctx, `SELECT request_id,stage FROM request_records WHERE stage IN('reserved','rerank_pending','dispatch_authorized','streaming') AND classification_after<=transaction_timestamp() ORDER BY classification_after,request_id FOR UPDATE SKIP LOCKED LIMIT $1`, limit)
	if err != nil {
		return SweepResult{}, fmt.Errorf("select classifications: %w", err)
	}
	type item struct{ id, stage string }
	items := make([]item, 0, limit)
	for rows.Next() {
		var value item
		if err := rows.Scan(&value.id, &value.stage); err != nil {
			rows.Close()
			return SweepResult{}, err
		}
		items = append(items, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return SweepResult{}, err
	}
	rows.Close()
	var result SweepResult
	for _, value := range items {
		var tenant []byte
		var cost int
		var grantState string
		if err := tx.QueryRow(ctx, `SELECT tenant_hash,slot_cost,state FROM admission_grants WHERE request_id=$1 FOR UPDATE`, value.id).Scan(&tenant, &cost, &grantState); err != nil {
			return SweepResult{}, err
		}
		var ignored int
		if err := tx.QueryRow(ctx, `SELECT active_grants+orphaned_grants FROM tenant_counters WHERE tenant_hash=$1 FOR UPDATE`, tenant).Scan(&ignored); err != nil {
			return SweepResult{}, err
		}
		var reservationID, state string
		var p domain.WorkloadIdentityParams
		err := tx.QueryRow(ctx, `SELECT reservation_id,state,cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch FROM reservations WHERE request_id=$1 AND is_current FOR UPDATE`, value.id).Scan(&reservationID, &state, &p.Cluster, &p.Namespace, &p.LogicalEngine, &p.PodUID, &p.EndpointEpoch, &p.RecoveryEpoch)
		if value.stage == "rerank_pending" {
			if err != nil && err != pgx.ErrNoRows {
				return SweepResult{}, err
			}
			if grantState != "retained_rerank" || (err == nil && state != "abandoned_rerank") {
				return SweepResult{}, domain.ErrInvalidState
			}
			if err == nil {
				proofHash := sha256Bytes("classification\x00" + reservationID)
				if _, err := tx.Exec(ctx, `UPDATE reservations
					SET state='released',is_current=false,terminal_proof_kind='classification',
					    terminal_proof_hash=$2,terminal_at=transaction_timestamp(),
					    updated_at=transaction_timestamp()
					WHERE reservation_id=$1 AND state='abandoned_rerank'`,
					reservationID, proofHash); err != nil {
					return SweepResult{}, err
				}
			}
			if err := releaseClassifiedGrant(ctx, tx, value.id, tenant, cost); err != nil {
				return SweepResult{}, err
			}
			if err := terminalizeClassifiedRequest(ctx, tx, value.id, "given_up"); err != nil {
				return SweepResult{}, err
			}
			result.GivenUp++
			continue
		}
		if err != nil {
			return SweepResult{}, err
		}
		identity, err := domain.NewWorkloadIdentity(p)
		if err != nil {
			return SweepResult{}, domain.ErrInvalidState
		}
		if err := tx.QueryRow(ctx, `SELECT reserved_slots FROM instance_capacity WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 FOR UPDATE`, identityArgs(identity)...).Scan(&ignored); err != nil {
			return SweepResult{}, err
		}
		if value.stage == "reserved" {
			if state != "reserved" || grantState != "active_reserved" {
				return SweepResult{}, domain.ErrInvalidState
			}
			args := append(identityArgs(identity), cost)
			if _, err := tx.Exec(ctx, `UPDATE instance_capacity SET reserved_slots=reserved_slots-$7,updated_at=transaction_timestamp() WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6`, args...); err != nil {
				return SweepResult{}, err
			}
			proofHash := sha256Bytes("classification\x00" + reservationID)
			if _, err := tx.Exec(ctx, `UPDATE reservations SET state='released',is_current=false,terminal_proof_kind='classification',terminal_proof_hash=$2,terminal_at=transaction_timestamp(),updated_at=transaction_timestamp() WHERE reservation_id=$1`, reservationID, proofHash); err != nil {
				return SweepResult{}, err
			}
			if err := releaseClassifiedGrant(ctx, tx, value.id, tenant, cost); err != nil {
				return SweepResult{}, err
			}
			if err := terminalizeClassifiedRequest(ctx, tx, value.id, "given_up"); err != nil {
				return SweepResult{}, err
			}
			result.GivenUp++
			continue
		}
		if state != "dispatch_authorized" && state != "streaming" {
			return SweepResult{}, domain.ErrInvalidState
		}
		args := append(identityArgs(identity), cost)
		if _, err := tx.Exec(ctx, `UPDATE instance_capacity SET reserved_slots=reserved_slots-$7,orphaned_slots=orphaned_slots+$7,updated_at=transaction_timestamp() WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6`, args...); err != nil {
			return SweepResult{}, err
		}
		if _, err := tx.Exec(ctx, `UPDATE tenant_counters SET active_grants=active_grants-$2,orphaned_grants=orphaned_grants+$2,version=version+1 WHERE tenant_hash=$1`, tenant, cost); err != nil {
			return SweepResult{}, err
		}
		if _, err := tx.Exec(ctx, `UPDATE admission_grants SET state='orphaned',updated_at=transaction_timestamp() WHERE request_id=$1 AND state='active_reserved'`, value.id); err != nil {
			return SweepResult{}, err
		}
		proofHash := sha256Bytes("classification\x00" + reservationID)
		if _, err := tx.Exec(ctx, `UPDATE reservations SET state='orphaned',is_current=false,terminal_proof_kind='classification',terminal_proof_hash=$2,terminal_at=transaction_timestamp(),updated_at=transaction_timestamp() WHERE reservation_id=$1`, reservationID, proofHash); err != nil {
			return SweepResult{}, err
		}
		debtID, err := randomID("debt_")
		if err != nil {
			return SweepResult{}, err
		}
		_, err = tx.Exec(ctx, `INSERT INTO orphaned_capacity_debts(debt_id,reservation_id,request_id,tenant_hash,cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,slot_cost,cause,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'classification_timeout','active')`, debtID, reservationID, value.id, tenant, identity.Cluster(), identity.Namespace(), identity.LogicalEngine(), identity.PodUID(), identity.EndpointEpoch(), identity.RecoveryEpoch(), cost)
		if err != nil {
			return SweepResult{}, err
		}
		if err := terminalizeClassifiedRequest(ctx, tx, value.id, "ambiguous"); err != nil {
			return SweepResult{}, err
		}
		result.ConvertedToDebt++
	}
	if err := tx.Commit(ctx); err != nil {
		return SweepResult{}, err
	}
	return result, nil
}

func releaseClassifiedGrant(ctx context.Context, tx pgx.Tx, requestID string, tenant []byte, cost int) error {
	tag, err := tx.Exec(ctx, `UPDATE admission_grants SET state='released',reservation_id=COALESCE(reservation_id,(SELECT reservation_id FROM reservations WHERE request_id=$1 ORDER BY request_generation DESC LIMIT 1)),updated_at=transaction_timestamp() WHERE request_id=$1 AND state IN('active_reserved','retained_rerank')`, requestID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	tag, err = tx.Exec(ctx, `UPDATE tenant_counters SET active_grants=active_grants-$2,version=version+1 WHERE tenant_hash=$1 AND active_grants >= $2`, tenant, cost)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	return nil
}
func terminalizeClassifiedRequest(ctx context.Context, tx pgx.Tx, requestID, outcome string) error {
	tag, err := tx.Exec(ctx, `UPDATE request_records SET stage='terminal',outcome=$2,terminal_at=transaction_timestamp(),updated_at=transaction_timestamp() WHERE request_id=$1 AND stage<>'terminal'`, requestID, outcome)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	return nil
}
