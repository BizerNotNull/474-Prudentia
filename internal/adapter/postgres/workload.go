package postgres

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/jackc/pgx/v5"
)

const maxKubernetesMutationCallLifetime = 30 * time.Second

func operationIntentName(intent domain.WorkloadOperationIntent) (string, error) {
	switch intent {
	case domain.OperationDrain:
		return "drain", nil
	case domain.OperationScaleDown:
		return "scale", nil
	case domain.OperationExactRemoval:
		return "delete", nil
	case domain.OperationRecovery:
		return "handoff_recovery", nil
	case domain.OperationHandoff:
		return "handoff_recovery", nil
	default:
		return "", domain.ErrInvalidState
	}
}

// AdvanceWorkloadOperationFence closes admission before issuing a new durable
// generation/token. Every affected capacity row is locked before its drain row.
func (c *Catalog) AdvanceWorkloadOperationFence(ctx context.Context, generation domain.WriterGeneration, scope domain.WorkloadRef, intent domain.WorkloadOperationIntent) (domain.WorkloadOperation, error) {
	intentName, err := operationIntentName(intent)
	if err != nil {
		return domain.WorkloadOperation{}, err
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.WorkloadOperation{}, fmt.Errorf("begin workload fence: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := lockWriter(ctx, tx, scope.Cluster(), generation); err != nil {
		return domain.WorkloadOperation{}, err
	}
	rows, err := tx.Query(ctx, `SELECT c.cluster_id,c.namespace,c.logical_engine,c.pod_uid,c.endpoint_epoch,c.recovery_epoch
		FROM instance_capacity c JOIN source_observations o USING(cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch)
		WHERE c.cluster_id=$1 AND o.source_kind='structural' AND o.normalized_payload->>'workload_uid'=$2
		ORDER BY c.namespace,c.logical_engine,c.pod_uid,c.endpoint_epoch,c.recovery_epoch FOR UPDATE OF c`, scope.Cluster(), scope.UID())
	if err != nil {
		return domain.WorkloadOperation{}, fmt.Errorf("lock workload capacity: %w", err)
	}
	var ids [][]any
	for rows.Next() {
		values := make([]any, 6)
		ptrs := make([]any, 6)
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			rows.Close()
			return domain.WorkloadOperation{}, err
		}
		ids = append(ids, values)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return domain.WorkloadOperation{}, err
	}
	rows.Close()
	for _, values := range ids {
		if _, err := tx.Exec(ctx, `UPDATE instance_capacity SET admission_limit=0,updated_at=transaction_timestamp() WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6`, values...); err != nil {
			return domain.WorkloadOperation{}, err
		}
	}
	var oldGeneration uint64
	err = tx.QueryRow(ctx, `SELECT operation_generation FROM workload_operations WHERE cluster_id=$1 AND workload_uid=$2 AND is_current FOR UPDATE`, scope.Cluster(), scope.UID()).Scan(&oldGeneration)
	if errors.Is(err, pgx.ErrNoRows) {
		oldGeneration = 0
	} else if err != nil {
		return domain.WorkloadOperation{}, fmt.Errorf("lock current workload operation: %w", err)
	}
	if oldGeneration == ^uint64(0) {
		return domain.WorkloadOperation{}, domain.ErrInvalidState
	}
	newGeneration := oldGeneration + 1
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return domain.WorkloadOperation{}, err
	}
	token := hex.EncodeToString(tokenBytes)
	if oldGeneration != 0 {
		if _, err := tx.Exec(ctx, `UPDATE workload_operations SET is_current=false,updated_at=transaction_timestamp() WHERE cluster_id=$1 AND workload_uid=$2 AND operation_generation=$3`, scope.Cluster(), scope.UID(), oldGeneration); err != nil {
			return domain.WorkloadOperation{}, err
		}
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT transaction_timestamp()`).Scan(&now); err != nil {
		return domain.WorkloadOperation{}, err
	}
	quiescent := now.Add(maxKubernetesMutationCallLifetime)
	_, err = tx.Exec(ctx, `INSERT INTO workload_operations(cluster_id,workload_uid,operation_generation,operation_token,writer_generation,intent,desired_replicas,phase,prior_workload_resource_version,old_calls_quiescent_after)
		VALUES($1,$2,$3,$4,$5,$6,$7,'barrier_pending',$8,$9)`, scope.Cluster(), scope.UID(), newGeneration, token, generation, intentName, scope.Replicas(), scope.ResourceVersion(), quiescent)
	if err != nil {
		return domain.WorkloadOperation{}, fmt.Errorf("insert workload operation: %w", err)
	}
	drainHash := sha256.Sum256([]byte(scope.Cluster() + "\x00" + scope.UID()))
	_, err = tx.Exec(ctx, `INSERT INTO drain_intents(drain_id,cluster_id,scope_kind,workload_uid,state,reason,writer_generation,operation_generation)
		VALUES($1,$2,'workload',$3,'barrier_pending',$4,$5,$6)
		ON CONFLICT(cluster_id,workload_uid) WHERE scope_kind='workload' AND state<>'cleared'
		DO UPDATE SET state='barrier_pending',reason=EXCLUDED.reason,writer_generation=EXCLUDED.writer_generation,operation_generation=EXCLUDED.operation_generation,updated_at=transaction_timestamp()`, hex.EncodeToString(drainHash[:]), scope.Cluster(), scope.UID(), intentName, generation, newGeneration)
	if err != nil {
		return domain.WorkloadOperation{}, fmt.Errorf("upsert workload drain: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WorkloadOperation{}, fmt.Errorf("commit workload fence: %w", err)
	}
	return domain.NewWorkloadOperation(domain.WorkloadOperationParams{Scope: scope, Intent: intent, Phase: domain.OperationBarrierPending, Generation: newGeneration, Token: token, OldCallsQuiescentAfter: quiescent})
}

func podResourceVersions(pods []domain.PodRef) ([]byte, error) {
	values := make(map[string]string, len(pods))
	for _, pod := range pods {
		token, ok := pod.OperationToken()
		if !ok || token == "" {
			return nil, domain.ErrInvalidState
		}
		values[pod.UID()] = pod.ResourceVersion()
	}
	return json.Marshal(values)
}

func (c *Catalog) RecordWorkloadBarrierObserved(ctx context.Context, generation domain.WriterGeneration, proof domain.WorkloadBarrierProof) error {
	ref := proof.Operation()
	workload := proof.Workload()
	if ref.WorkloadUID() != workload.UID() {
		return domain.ErrInvalidState
	}
	podVersions, err := podResourceVersions(proof.Pods())
	if err != nil {
		return err
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockWriter(ctx, tx, workload.Cluster(), generation); err != nil {
		return err
	}
	var token, phase string
	var currentGeneration uint64
	if err := tx.QueryRow(ctx, `SELECT operation_generation,operation_token,phase FROM workload_operations WHERE cluster_id=$1 AND workload_uid=$2 AND is_current FOR UPDATE`, workload.Cluster(), workload.UID()).Scan(&currentGeneration, &token, &phase); err != nil {
		return fmt.Errorf("lock workload barrier: %w", err)
	}
	if currentGeneration != ref.Generation() || token != ref.Token() || phase != "barrier_pending" {
		return domain.ErrInvalidState
	}
	tag, err := tx.Exec(ctx, `UPDATE workload_operations SET phase='barrier_observed',current_workload_resource_version=$4,pod_token_resource_versions=$5,barrier_observed_at=transaction_timestamp(),updated_at=transaction_timestamp() WHERE cluster_id=$1 AND workload_uid=$2 AND operation_generation=$3 AND phase='barrier_pending'`, workload.Cluster(), workload.UID(), ref.Generation(), workload.ResourceVersion(), podVersions)
	if err != nil || tag.RowsAffected() != 1 {
		if err != nil {
			return err
		}
		return domain.ErrInvalidState
	}
	_, err = tx.Exec(ctx, `UPDATE drain_intents SET state='barrier_observed',updated_at=transaction_timestamp() WHERE cluster_id=$1 AND workload_uid=$2 AND operation_generation=$3 AND state='barrier_pending'`, workload.Cluster(), workload.UID(), ref.Generation())
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Catalog) ObserveWorkloadVictims(ctx context.Context, generation domain.WriterGeneration, observation domain.WorkloadVictimObservation) error {
	ref := observation.Operation()
	before := observation.Before().Values()
	actual := append(observation.Terminating().Values(), observation.Disappeared().Values()...)
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var cluster string
	if err := tx.QueryRow(ctx, `SELECT cluster_id FROM workload_operations WHERE workload_uid=$1 AND operation_generation=$2 AND operation_token=$3 AND is_current`, ref.WorkloadUID(), ref.Generation(), ref.Token()).Scan(&cluster); err != nil {
		return domain.ErrInvalidReference
	}
	if err := lockWriter(ctx, tx, cluster, generation); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE workload_operations SET phase='observing_victims',before_victim_uids=$4,actual_victim_uids=$5,updated_at=transaction_timestamp() WHERE cluster_id=$1 AND workload_uid=$2 AND operation_generation=$3 AND phase IN('barrier_observed','mutation_pending','observing_victims')`, cluster, ref.WorkloadUID(), ref.Generation(), before, actual)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	return tx.Commit(ctx)
}

func completionHash(proof domain.WorkloadCompletionProof) [32]byte {
	hash := sha256.New()
	ref := proof.Barrier().Operation()
	fmt.Fprintf(hash, "%s\x00%d\x00%s\x00", ref.WorkloadUID(), ref.Generation(), ref.Token())
	for _, pod := range proof.CurrentPods() {
		fmt.Fprintf(hash, "%s\x00%s\x00", pod.UID(), pod.ResourceVersion())
	}
	var out [32]byte
	copy(out[:], hash.Sum(nil))
	return out
}

func (c *Catalog) CompleteWorkloadOperationAndReopen(ctx context.Context, generation domain.WriterGeneration, proof domain.WorkloadCompletionProof) error {
	barrier := proof.Barrier()
	ref := barrier.Operation()
	workload := barrier.Workload()
	hash := completionHash(proof)
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockWriter(ctx, tx, workload.Cluster(), generation); err != nil {
		return err
	}
	var phase, token string
	var quiescent, now time.Time
	if err := tx.QueryRow(ctx, `SELECT phase,operation_token,old_calls_quiescent_after,transaction_timestamp() FROM workload_operations WHERE cluster_id=$1 AND workload_uid=$2 AND operation_generation=$3 AND is_current FOR UPDATE`, workload.Cluster(), workload.UID(), ref.Generation()).Scan(&phase, &token, &quiescent, &now); err != nil {
		return domain.ErrInvalidReference
	}
	if token != ref.Token() || phase != "observing_victims" || now.Before(quiescent) {
		return domain.ErrInvalidState
	}
	var activeDebt int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM orphaned_capacity_debts d JOIN source_observations o USING(cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch) WHERE d.state='active' AND o.source_kind='structural' AND o.normalized_payload->>'workload_uid'=$1`, workload.UID()).Scan(&activeDebt); err != nil {
		return err
	}
	if activeDebt != 0 {
		return domain.ErrInvalidState
	}
	_, err = tx.Exec(ctx, `UPDATE workload_operations SET phase='completed',completion_proof_hash=$4,completed_at=transaction_timestamp(),updated_at=transaction_timestamp() WHERE cluster_id=$1 AND workload_uid=$2 AND operation_generation=$3`, workload.Cluster(), workload.UID(), ref.Generation(), hash[:])
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE drain_intents SET state='cleared',cleared_at=transaction_timestamp(),updated_at=transaction_timestamp() WHERE cluster_id=$1 AND workload_uid=$2 AND operation_generation=$3 AND state='barrier_observed'`, workload.Cluster(), workload.UID(), ref.Generation())
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE instance_capacity c SET admission_limit=physical_slots,updated_at=transaction_timestamp() FROM source_observations o WHERE c.cluster_id=o.cluster_id AND c.namespace=o.namespace AND c.logical_engine=o.logical_engine AND c.pod_uid=o.pod_uid AND c.endpoint_epoch=o.endpoint_epoch AND c.recovery_epoch=o.recovery_epoch AND o.source_kind='structural' AND o.normalized_payload->>'workload_uid'=$1 AND NOT c.retired`, workload.UID())
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
