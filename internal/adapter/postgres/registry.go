package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	registryapp "github.com/BizerNotNull/474-Prudentia/internal/registry"
	"github.com/jackc/pgx/v5"
)

type observationRow struct {
	generation, sequence uint64
	accepted, expires    time.Time
	payload              []byte
}

func scanObservation(ctx context.Context, tx pgx.Tx, id domain.WorkloadIdentity, kind string) (observationRow, error) {
	var row observationRow
	err := tx.QueryRow(ctx, `SELECT writer_generation,source_sequence,accepted_at,expires_at,normalized_payload FROM source_observations WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 AND source_kind=$7`, append(identityArgs(id), kind)...).Scan(&row.generation, &row.sequence, &row.accepted, &row.expires, &row.payload)
	return row, err
}
func storedStamp(row observationRow, kind domain.SourceKind, id domain.WorkloadIdentity) (domain.StoredSourceStamp, error) {
	sequence, err := domain.NewSourceSequence(row.sequence)
	if err != nil {
		return domain.StoredSourceStamp{}, err
	}
	source, err := domain.NewSourceStamp(kind, domain.WriterGeneration(row.generation), sequence)
	if err != nil {
		return domain.StoredSourceStamp{}, err
	}
	return domain.NewStoredSourceStamp(domain.StoredSourceStampParams{Source: source, Identity: id, Version: row.sequence, AcceptedAt: row.accepted, ExpiresAt: row.expires})
}

func projectInTx(ctx context.Context, tx pgx.Tx, id domain.WorkloadIdentity, asOf time.Time) (domain.InstanceSnapshot, error) {
	var physical, reserved, orphaned uint32
	var version uint64
	var retired bool
	if err := tx.QueryRow(ctx, `SELECT physical_slots,reserved_slots,orphaned_slots,projection_version,retired FROM instance_capacity WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6`, identityArgs(id)...).Scan(&physical, &reserved, &orphaned, &version, &retired); err != nil {
		return domain.InstanceSnapshot{}, fmt.Errorf("read projected capacity: %w", err)
	}
	structural, err := scanObservation(ctx, tx, id, "structural")
	if err != nil {
		return domain.InstanceSnapshot{}, fmt.Errorf("read structural observation: %w", err)
	}
	health, err := scanObservation(ctx, tx, id, "runtime_health")
	if err != nil {
		return domain.InstanceSnapshot{}, fmt.Errorf("read health observation: %w", err)
	}
	if !asOf.Before(structural.expires) || !asOf.Before(health.expires) {
		return domain.InstanceSnapshot{}, domain.ErrNoCapacity
	}
	var structure struct{ Endpoint, Model string }
	if err := json.Unmarshal(structural.payload, &structure); err != nil {
		return domain.InstanceSnapshot{}, domain.ErrInvalidState
	}
	var healthPayload struct {
		State uint8
		Warm  bool
	}
	if err := json.Unmarshal(health.payload, &healthPayload); err != nil {
		return domain.InstanceSnapshot{}, domain.ErrInvalidState
	}
	endpoint, err := domain.NewEndpointRef(structure.Endpoint)
	if err != nil {
		return domain.InstanceSnapshot{}, err
	}
	modelKey, err := domain.NewModelKey(structure.Model)
	if err != nil {
		return domain.InstanceSnapshot{}, err
	}
	model, err := domain.NewModelFingerprint(modelKey, "observed")
	if err != nil {
		return domain.InstanceSnapshot{}, err
	}
	structuralStamp, err := storedStamp(structural, domain.SourceStructural, id)
	if err != nil {
		return domain.InstanceSnapshot{}, err
	}
	healthStamp, err := storedStamp(health, domain.SourceRuntimeHealth, id)
	if err != nil {
		return domain.InstanceSnapshot{}, err
	}
	params := domain.SnapshotParams{Identity: id, Endpoint: endpoint, Model: model, Capabilities: domain.EmptyFeatureSet(), Structural: structuralStamp, Health: healthStamp, HealthState: domain.HealthState(healthPayload.State), DrainState: domain.DrainReady, ConfiguredSlots: physical, ReservedSlots: reserved, OrphanedSlots: orphaned, ProjectionVersion: version, CatalogAsOf: asOf}
	var drain bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM drain_intents WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 AND state<>'cleared')`, identityArgs(id)...).Scan(&drain); err != nil {
		return domain.InstanceSnapshot{}, err
	}
	if drain || retired {
		params.DrainState = domain.DrainActive
	}
	load, loadErr := scanObservation(ctx, tx, id, "load")
	if loadErr == nil && asOf.Before(load.expires) {
		var payload struct{ Utilization *float64 }
		if json.Unmarshal(load.payload, &payload) == nil && payload.Utilization != nil && *payload.Utilization >= 0 && *payload.Utilization <= 1 {
			loadStamp, err := storedStamp(load, domain.SourceLoad, id)
			if err != nil {
				return domain.InstanceSnapshot{}, err
			}
			advisory, err := domain.NewAdvisoryLoad(uint16(*payload.Utilization * 10000))
			if err != nil {
				return domain.InstanceSnapshot{}, err
			}
			params.Load = loadStamp
			params.HasLoadStamp = true
			params.AdvisoryLoad = advisory
			params.HasAdvisoryLoad = true
		}
	} else if loadErr != nil && !errors.Is(loadErr, pgx.ErrNoRows) {
		return domain.InstanceSnapshot{}, loadErr
	}
	return domain.NewInstanceSnapshot(params)
}

func (c *Catalog) ProjectSnapshot(ctx context.Context, id domain.WorkloadIdentity) (domain.InstanceSnapshot, error) {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.InstanceSnapshot{}, err
	}
	defer tx.Rollback(ctx)
	var asOf time.Time
	if err := tx.QueryRow(ctx, `SELECT transaction_timestamp()`).Scan(&asOf); err != nil {
		return domain.InstanceSnapshot{}, err
	}
	snapshot, err := projectInTx(ctx, tx, id, asOf)
	if err != nil {
		return domain.InstanceSnapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.InstanceSnapshot{}, err
	}
	return snapshot, nil
}

func (c *Catalog) ListCandidateSnapshots(ctx context.Context, q registryapp.CandidateQuery) (domain.CandidateCatalog, error) {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.CandidateCatalog{}, err
	}
	defer tx.Rollback(ctx)
	var asOf time.Time
	if err := tx.QueryRow(ctx, `SELECT transaction_timestamp()`).Scan(&asOf); err != nil {
		return domain.CandidateCatalog{}, err
	}
	rows, err := tx.Query(ctx, `SELECT DISTINCT c.cluster_id,c.namespace,c.logical_engine,c.pod_uid,c.endpoint_epoch,c.recovery_epoch FROM instance_capacity c JOIN source_observations s USING(cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch) JOIN source_observations h USING(cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch) WHERE NOT c.retired AND s.source_kind='structural' AND h.source_kind='runtime_health' AND s.expires_at>$1 AND h.expires_at>$1 AND s.normalized_payload->>'model'=$2 ORDER BY c.cluster_id,c.namespace,c.logical_engine,c.pod_uid,c.endpoint_epoch,c.recovery_epoch LIMIT $3`, asOf, q.Model, q.Limit)
	if err != nil {
		return domain.CandidateCatalog{}, fmt.Errorf("list candidate identities: %w", err)
	}
	var identities []domain.WorkloadIdentity
	for rows.Next() {
		var p domain.WorkloadIdentityParams
		if err := rows.Scan(&p.Cluster, &p.Namespace, &p.LogicalEngine, &p.PodUID, &p.EndpointEpoch, &p.RecoveryEpoch); err != nil {
			rows.Close()
			return domain.CandidateCatalog{}, err
		}
		id, err := domain.NewWorkloadIdentity(p)
		if err != nil {
			rows.Close()
			return domain.CandidateCatalog{}, err
		}
		identities = append(identities, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return domain.CandidateCatalog{}, err
	}
	rows.Close()
	snapshots := make([]domain.InstanceSnapshot, 0, len(identities))
	for _, id := range identities {
		snapshot, err := projectInTx(ctx, tx, id, asOf)
		if errors.Is(err, domain.ErrNoCapacity) {
			continue
		}
		if err != nil {
			return domain.CandidateCatalog{}, err
		}
		if snapshot.Model().Model().String() != q.Model || !snapshot.Capabilities().Contains(q.Required) {
			continue
		}
		snapshots = append(snapshots, snapshot)
	}
	catalog, err := domain.NewCandidateCatalog(snapshots, asOf)
	if err != nil {
		return domain.CandidateCatalog{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.CandidateCatalog{}, err
	}
	return catalog, nil
}
