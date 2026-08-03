package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ControllerCatalog struct{ *Catalog }

func NewControllerCatalog(pool *pgxpool.Pool) (*ControllerCatalog, error) {
	if pool == nil {
		return nil, errors.New("invalid controller catalog configuration")
	}
	key := make([]byte, 32)
	key[0] = 1
	keyring, _ := NewLocalCapabilityKeyring(map[uint32][]byte{1: key}, map[uint32][]byte{1: key})
	catalog, err := NewCatalog(pool, keyring)
	if err != nil {
		return nil, err
	}
	return catalog.ControllerCatalog(), nil
}

func (c *ControllerCatalog) AcquireControllerWriterGeneration(ctx context.Context, cluster, holder string) (domain.WriterGeneration, error) {
	if cluster == "" || holder == "" || len(cluster) > 128 || len(holder) > 256 {
		return 0, errors.New("invalid controller identity")
	}
	var generation uint64
	err := c.pool.QueryRow(ctx, `INSERT INTO controller_writer_generations(cluster,current_generation,holder,acquired_at) VALUES($1,1,$2,transaction_timestamp()) ON CONFLICT(cluster) DO UPDATE SET current_generation=controller_writer_generations.current_generation+1,holder=EXCLUDED.holder,acquired_at=transaction_timestamp() RETURNING current_generation`, cluster, holder).Scan(&generation)
	if err != nil {
		return 0, fmt.Errorf("advance controller writer generation: %w", err)
	}
	return domain.WriterGeneration(generation), nil
}

// RecordObservations is mandatory on the production controller facet. Each
// observation uses Catalog.RecordObservation, including its generation fence
// and database-derived accepted/expiry timestamps.
func (c *ControllerCatalog) RecordObservations(ctx context.Context, generation domain.WriterGeneration, _ domain.ResourceKey, observations []domain.Observation) error {
	for _, observation := range observations {
		if _, _, err := c.Catalog.RecordObservation(ctx, generation, observation); err != nil {
			return err
		}
	}
	return nil
}

// RecordDesiredApply generation-fences the acknowledgement. Workload-operation
// detail is persisted by the workload operation APIs, never controller memory.
func (c *ControllerCatalog) RecordDesiredApply(ctx context.Context, generation domain.WriterGeneration, result domain.ApplyResult) error {
	ref := result.Workload()
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockWriter(ctx, tx, ref.Cluster(), generation); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ReplaceResourceProjection projects already-recorded observations into the
// same instance_capacity/instance_projections rows used by reservation and
// dispatch. It never writes scheduler_backends.
func (c *ControllerCatalog) ReplaceResourceProjection(ctx context.Context, generation domain.WriterGeneration, state domain.ResourceState) error {
	if generation == 0 {
		return domain.ErrStaleWriterGeneration
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockWriter(ctx, tx, state.Cluster(), generation); err != nil {
		return err
	}
	key := state.Key()
	projections := state.Projections()
	seen := make([]string, 0, len(projections))
	for _, projection := range projections {
		id := projection.Identity()
		seen = append(seen, id.PodUID())
		var reserved, orphaned int
		var version uint64
		err := tx.QueryRow(ctx, `SELECT reserved_slots,orphaned_slots,projection_version FROM instance_capacity WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 FOR UPDATE`, identityArgs(id)...).Scan(&reserved, &orphaned, &version)
		if errors.Is(err, pgx.ErrNoRows) {
			reserved, orphaned, version = 0, 0, 0
		} else if err != nil {
			return err
		}
		limit := int(projection.ConfiguredSlots())
		draining, err := lockApplicableDrains(ctx, tx, id)
		if err != nil {
			return err
		}
		if draining {
			limit = 0
		}
		version++
		_, err = tx.Exec(ctx, `INSERT INTO instance_capacity(cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,physical_slots,admission_limit,projection_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch) DO UPDATE SET physical_slots=GREATEST(EXCLUDED.physical_slots,instance_capacity.reserved_slots+instance_capacity.orphaned_slots),admission_limit=EXCLUDED.admission_limit,projection_version=EXCLUDED.projection_version,retired=false,updated_at=transaction_timestamp()`, id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch(), projection.ConfiguredSlots(), limit, version)
		if err != nil {
			return err
		}
		modelHash := sha256.Sum256([]byte(projection.Model()))
		configHash := sha256.Sum256([]byte(id.Cluster() + "\x00" + id.Namespace() + "\x00" + id.LogicalEngine()))
		memberHash := sha256.Sum256([]byte(id.PodUID()))
		var manifestID string
		var manifestVersion uint64
		if err := tx.QueryRow(ctx, `SELECT manifest_id,manifest_version FROM capability_manifests WHERE valid_from<=transaction_timestamp() AND valid_until>transaction_timestamp() ORDER BY manifest_version DESC LIMIT 1`).Scan(&manifestID, &manifestVersion); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO instance_projections(cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,normalized_proxy_endpoint,model_fingerprint,config_fingerprint,membership_fingerprint,capability_manifest_id,capability_manifest_version,source_stamps,health,projection_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'{}','healthy',$13) ON CONFLICT(cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch) DO UPDATE SET normalized_proxy_endpoint=EXCLUDED.normalized_proxy_endpoint,model_fingerprint=EXCLUDED.model_fingerprint,config_fingerprint=EXCLUDED.config_fingerprint,membership_fingerprint=EXCLUDED.membership_fingerprint,capability_manifest_id=EXCLUDED.capability_manifest_id,capability_manifest_version=EXCLUDED.capability_manifest_version,health=EXCLUDED.health,projection_version=EXCLUDED.projection_version,projected_at=transaction_timestamp()`, id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch(), projection.Endpoint(), modelHash[:], configHash[:], memberHash[:], manifestID, manifestVersion, version)
		if err != nil {
			return err
		}
	}
	if len(seen) == 0 {
		_, err = tx.Exec(ctx, `UPDATE instance_capacity c SET admission_limit=0,retired=true,updated_at=transaction_timestamp() FROM source_observations o WHERE c.cluster_id=$1 AND o.cluster_id=c.cluster_id AND o.namespace=c.namespace AND o.logical_engine=c.logical_engine AND o.pod_uid=c.pod_uid AND o.endpoint_epoch=c.endpoint_epoch AND o.recovery_epoch=c.recovery_epoch AND o.source_kind='structural' AND o.normalized_payload->>'source_namespace'=$2 AND o.normalized_payload->>'source_name'=$3`, state.Cluster(), key.Namespace(), key.Name())
	} else {
		_, err = tx.Exec(ctx, `UPDATE instance_capacity SET admission_limit=0,retired=true,updated_at=transaction_timestamp() WHERE cluster_id=$1 AND namespace=$2 AND pod_uid<>ALL($3)`, state.Cluster(), key.Namespace(), seen)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
