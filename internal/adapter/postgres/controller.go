package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ControllerCatalog struct {
	pool *pgxpool.Pool
}

func NewControllerCatalog(pool *pgxpool.Pool) (*ControllerCatalog, error) {
	if pool == nil {
		return nil, errors.New("invalid controller catalog configuration")
	}
	return &ControllerCatalog{pool: pool}, nil
}

func (c *ControllerCatalog) AcquireControllerWriterGeneration(ctx context.Context, cluster, holder string) (domain.WriterGeneration, error) {
	if cluster == "" || holder == "" || len(cluster) > 128 || len(holder) > 256 {
		return 0, errors.New("invalid controller identity")
	}
	var generation uint64
	err := c.pool.QueryRow(ctx, `
		INSERT INTO controller_writer_generations (cluster, current_generation, holder, acquired_at)
		VALUES ($1, 1, $2, clock_timestamp())
		ON CONFLICT (cluster) DO UPDATE SET
			current_generation = controller_writer_generations.current_generation + 1,
			holder = EXCLUDED.holder,
			acquired_at = clock_timestamp()
		RETURNING current_generation`, cluster, holder).Scan(&generation)
	if err != nil {
		return 0, fmt.Errorf("advance controller writer generation: %w", err)
	}
	return domain.WriterGeneration(generation), nil
}

func (c *ControllerCatalog) ReplaceResourceProjection(ctx context.Context, generation domain.WriterGeneration, state domain.ResourceState) error {
	if generation == 0 {
		return domain.ErrStaleWriterGeneration
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("begin projection replacement: %w", err)
	}
	defer tx.Rollback(ctx)

	var current uint64
	err = tx.QueryRow(ctx, `SELECT current_generation FROM controller_writer_generations WHERE cluster=$1 FOR UPDATE`, state.Cluster()).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) || current != uint64(generation) {
		return domain.ErrStaleWriterGeneration
	}
	if err != nil {
		return fmt.Errorf("check controller writer generation: %w", err)
	}

	key := state.Key()
	projections := state.Projections()
	for _, projection := range projections {
		identity := projection.Identity()
		_, err := tx.Exec(ctx, `
			INSERT INTO scheduler_backends (
				cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch,
				model, endpoint, configured_slots, admission_limit, healthy, drain_active,
				eligible_until, source_namespace, source_name)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9,true,false,
				clock_timestamp()+($10 * interval '1 millisecond'),$11,$12)
			ON CONFLICT (cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch)
			DO UPDATE SET
				model=EXCLUDED.model,
				endpoint=EXCLUDED.endpoint,
				configured_slots=GREATEST(EXCLUDED.configured_slots, scheduler_backends.reserved_slots+scheduler_backends.orphaned_slots),
				admission_limit=EXCLUDED.admission_limit,
				healthy=true,
				drain_active=false,
				eligible_until=EXCLUDED.eligible_until,
				source_namespace=EXCLUDED.source_namespace,
				source_name=EXCLUDED.source_name`,
			identity.Cluster(), identity.Namespace(), identity.LogicalEngine(), identity.PodUID(), identity.EndpointEpoch(), identity.RecoveryEpoch(),
			projection.Model(), projection.Endpoint(), projection.ConfiguredSlots(), projection.FreshFor().Milliseconds(), key.Namespace(), key.Name())
		if err != nil {
			return fmt.Errorf("upsert backend projection: %w", err)
		}
	}

	if len(projections) == 0 {
		_, err = tx.Exec(ctx, `UPDATE scheduler_backends SET healthy=false, admission_limit=0, drain_active=true
			WHERE cluster=$1 AND source_namespace=$2 AND source_name=$3`, state.Cluster(), key.Namespace(), key.Name())
	} else {
		identity := projections[0].Identity()
		_, err = tx.Exec(ctx, `UPDATE scheduler_backends SET healthy=false, admission_limit=0, drain_active=true
			WHERE cluster=$1 AND source_namespace=$2 AND source_name=$3
			AND (pod_uid, endpoint_epoch, recovery_epoch) <> ($4,$5,$6)`, identity.Cluster(), key.Namespace(), key.Name(), identity.PodUID(), identity.EndpointEpoch(), identity.RecoveryEpoch())
	}
	if err != nil {
		return fmt.Errorf("retire stale backend projections: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit projection replacement: %w", err)
	}
	return nil
}
