package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/jackc/pgx/v5"
)

var observationTTLs = map[domain.TTLClass]time.Duration{
	domain.TTLStructural:    5 * time.Minute,
	domain.TTLRuntimeHealth: 30 * time.Second,
	domain.TTLLoad:          10 * time.Second,
}

func sourceKindName(kind domain.SourceKind) (string, error) {
	switch kind {
	case domain.SourceStructural:
		return "structural", nil
	case domain.SourceRuntimeHealth:
		return "runtime_health", nil
	case domain.SourceLoad:
		return "load", nil
	default:
		return "", domain.ErrInvalidState
	}
}

func observationPayload(o domain.Observation) ([]byte, error) {
	if structural, ok := o.Structural(); ok {
		workload := structural.Workload()
		return json.Marshal(map[string]any{
			"endpoint": structural.Endpoint(), "model": structural.Model(), "workload_uid": workload.UID(),
			"endpoint_epoch": structural.EndpointEpoch(), "recovery_epoch": structural.RecoveryEpoch(),
		})
	}
	if health, ok := o.RuntimeHealth(); ok {
		return json.Marshal(map[string]any{"state": uint8(health.State()), "warm": health.Warm()})
	}
	if load, ok := o.Load(); ok {
		payload := map[string]any{"running": load.RunningRequests(), "queued": load.QueuedRequests()}
		if utilization, ok := load.Utilization(); ok {
			payload["utilization"] = utilization
		}
		return json.Marshal(payload)
	}
	return nil, domain.ErrInvalidState
}

func (c *Catalog) AcquireControllerWriterGeneration(ctx context.Context, cluster, holder string) (domain.WriterGeneration, error) {
	return c.ControllerCatalog().AcquireControllerWriterGeneration(ctx, cluster, holder)
}

// RecordObservation accepts only a current database writer and a strictly
// newer source sequence. accepted_at and expiry are both derived from the one
// transaction timestamp; observer clocks never participate in freshness.
func (c *Catalog) RecordObservation(ctx context.Context, generation domain.WriterGeneration, o domain.Observation) (domain.StoredSourceStamp, bool, error) {
	if generation == 0 || o.Stamp().WriterGeneration() != generation {
		return domain.StoredSourceStamp{}, false, domain.ErrStaleWriterGeneration
	}
	ttl, ok := observationTTLs[o.TTLClass()]
	if !ok {
		return domain.StoredSourceStamp{}, false, domain.ErrInvalidState
	}
	kind, err := sourceKindName(o.Stamp().Kind())
	if err != nil {
		return domain.StoredSourceStamp{}, false, err
	}
	payload, err := observationPayload(o)
	if err != nil {
		return domain.StoredSourceStamp{}, false, err
	}
	id := o.Identity()
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.StoredSourceStamp{}, false, fmt.Errorf("begin observation record: %w", err)
	}
	defer tx.Rollback(ctx)
	var now time.Time
	var current uint64
	if err := tx.QueryRow(ctx, `SELECT transaction_timestamp(), current_generation
		FROM controller_writer_generations WHERE cluster=$1 FOR UPDATE`, id.Cluster()).Scan(&now, &current); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.StoredSourceStamp{}, false, domain.ErrStaleWriterGeneration
		}
		return domain.StoredSourceStamp{}, false, fmt.Errorf("lock observation writer generation: %w", err)
	}
	if current != uint64(generation) {
		return domain.StoredSourceStamp{}, false, domain.ErrStaleWriterGeneration
	}
	var sourceTime *time.Time
	if supplied, ok := o.SourceReportedAt(); ok {
		sourceTime = &supplied
	}
	expires := now.Add(ttl)
	var acceptedAt, expiresAt time.Time
	var sequence uint64
	err = tx.QueryRow(ctx, `INSERT INTO source_observations (
		cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,source_kind,
		writer_generation,source_sequence,accepted_at,expires_at,ttl_policy_version,schema_version,
		normalized_payload,diagnostic_source_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,1,1,$12,$13)
		ON CONFLICT (cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,source_kind)
		DO UPDATE SET writer_generation=EXCLUDED.writer_generation,source_sequence=EXCLUDED.source_sequence,
		accepted_at=EXCLUDED.accepted_at,expires_at=EXCLUDED.expires_at,
		normalized_payload=EXCLUDED.normalized_payload,diagnostic_source_time=EXCLUDED.diagnostic_source_time
		WHERE source_observations.writer_generation < EXCLUDED.writer_generation
		   OR (source_observations.writer_generation = EXCLUDED.writer_generation
		       AND source_observations.source_sequence < EXCLUDED.source_sequence)
		RETURNING source_sequence,accepted_at,expires_at`,
		id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch(),
		kind, generation, o.Stamp().Sequence().Uint64(), now, expires, payload, sourceTime).Scan(&sequence, &acceptedAt, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.StoredSourceStamp{}, false, nil
	}
	if err != nil {
		return domain.StoredSourceStamp{}, false, fmt.Errorf("upsert observation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.StoredSourceStamp{}, false, fmt.Errorf("commit observation: %w", err)
	}
	stamp, err := domain.NewStoredSourceStamp(domain.StoredSourceStampParams{Source: o.Stamp(), Identity: id, Version: sequence, AcceptedAt: acceptedAt, ExpiresAt: expiresAt})
	return stamp, err == nil, err
}

func identityArgs(id domain.WorkloadIdentity) []any {
	return []any{id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch()}
}

func lockWriter(ctx context.Context, tx pgx.Tx, cluster string, generation domain.WriterGeneration) error {
	var current uint64
	if err := tx.QueryRow(ctx, `SELECT current_generation FROM controller_writer_generations WHERE cluster=$1 FOR UPDATE`, cluster).Scan(&current); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrStaleWriterGeneration
		}
		return fmt.Errorf("lock controller writer: %w", err)
	}
	if current != uint64(generation) {
		return domain.ErrStaleWriterGeneration
	}
	return nil
}

// SyncCapacityProjection locks the writer and exact capacity before projection.
// A lower admission limit is rejected while it would fall below reserved plus
// orphaned contribution; no counter is ever reduced by projection.
func (c *Catalog) SyncCapacityProjection(ctx context.Context, generation domain.WriterGeneration, p domain.ProjectionUpdate) (domain.ProjectionVersion, error) {
	id := p.Identity()
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin projection sync: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := lockWriter(ctx, tx, id.Cluster(), generation); err != nil {
		return 0, err
	}
	args := identityArgs(id)
	var reserved, orphaned int
	var currentVersion uint64
	err = tx.QueryRow(ctx, `SELECT reserved_slots,orphaned_slots,projection_version FROM instance_capacity
		WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 FOR UPDATE`, args...).Scan(&reserved, &orphaned, &currentVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		currentVersion = 0
	} else if err != nil {
		return 0, fmt.Errorf("lock exact capacity: %w", err)
	}
	workloadDraining, err := lockApplicableDrains(ctx, tx, id)
	if err != nil {
		return 0, fmt.Errorf("lock applicable drain intents: %w", err)
	}
	admissionLimit := p.AdmissionLimit()
	if workloadDraining {
		admissionLimit = 0
	}
	if uint64(p.PreviousVersion()) != currentVersion ||
		(!workloadDraining && uint64(admissionLimit) < uint64(reserved+orphaned)) ||
		uint64(p.ConfiguredSlots()) < uint64(reserved+orphaned) {
		return 0, domain.ErrInvalidState
	}
	newVersion := currentVersion + 1
	_, err = tx.Exec(ctx, `INSERT INTO instance_capacity (cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,physical_slots,admission_limit,projection_version)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch)
		DO UPDATE SET physical_slots=EXCLUDED.physical_slots,admission_limit=EXCLUDED.admission_limit,
		projection_version=EXCLUDED.projection_version,retired=false,updated_at=transaction_timestamp()`,
		id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch(), p.ConfiguredSlots(), admissionLimit, newVersion)
	if err != nil {
		return 0, fmt.Errorf("sync exact capacity: %w", err)
	}
	var structuralPayload, healthPayload []byte
	if err := tx.QueryRow(ctx, `SELECT normalized_payload FROM source_observations
		WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4
		  AND endpoint_epoch=$5 AND recovery_epoch=$6 AND source_kind='structural'`,
		identityArgs(id)...).Scan(&structuralPayload); err != nil {
		return 0, fmt.Errorf("read structural projection fact: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT normalized_payload FROM source_observations
		WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4
		  AND endpoint_epoch=$5 AND recovery_epoch=$6 AND source_kind='runtime_health'`,
		identityArgs(id)...).Scan(&healthPayload); err != nil {
		return 0, fmt.Errorf("read health projection fact: %w", err)
	}
	var structural struct{ Endpoint, Model string }
	var health struct{ State uint8 }
	if json.Unmarshal(structuralPayload, &structural) != nil || json.Unmarshal(healthPayload, &health) != nil ||
		structural.Endpoint == "" || structural.Model == "" {
		return 0, domain.ErrInvalidState
	}
	healthName := "unhealthy"
	if domain.HealthState(health.State) == domain.HealthReady {
		healthName = "healthy"
	}
	var manifestID string
	var manifestVersion uint64
	if err := tx.QueryRow(ctx, `SELECT manifest_id,manifest_version FROM capability_manifests
		WHERE valid_from<=transaction_timestamp() AND valid_until>transaction_timestamp()
		ORDER BY manifest_version DESC,manifest_id LIMIT 1`).Scan(&manifestID, &manifestVersion); err != nil {
		return 0, fmt.Errorf("select capability manifest: %w", err)
	}
	modelFingerprint := sha256.Sum256([]byte(structural.Model))
	configFingerprint := sha256.Sum256([]byte(id.Cluster() + "\x00" + id.Namespace() + "\x00" + id.LogicalEngine()))
	membershipFingerprint := sha256.Sum256([]byte(id.PodUID()))
	stamps, _ := json.Marshal(map[string]uint64{
		"structural": p.StructuralStamp().Version(),
		"health":     p.HealthStamp().Version(),
	})
	_, err = tx.Exec(ctx, `INSERT INTO instance_projections(
		cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,
		normalized_proxy_endpoint,model_fingerprint,config_fingerprint,membership_fingerprint,
		capability_manifest_id,capability_manifest_version,source_stamps,health,projection_version)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		ON CONFLICT(cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch)
		DO UPDATE SET normalized_proxy_endpoint=EXCLUDED.normalized_proxy_endpoint,
		model_fingerprint=EXCLUDED.model_fingerprint,config_fingerprint=EXCLUDED.config_fingerprint,
		membership_fingerprint=EXCLUDED.membership_fingerprint,
		capability_manifest_id=EXCLUDED.capability_manifest_id,
		capability_manifest_version=EXCLUDED.capability_manifest_version,
		source_stamps=EXCLUDED.source_stamps,health=EXCLUDED.health,
		projection_version=EXCLUDED.projection_version,projected_at=transaction_timestamp()`,
		id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch(),
		structural.Endpoint, modelFingerprint[:], configFingerprint[:], membershipFingerprint[:],
		manifestID, manifestVersion, stamps, healthName, newVersion)
	if err != nil {
		return 0, fmt.Errorf("sync instance projection: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit projection sync: %w", err)
	}
	return domain.ProjectionVersion(newVersion), nil
}

func (c *Catalog) RetireCapacityProjection(ctx context.Context, generation domain.WriterGeneration, id domain.WorkloadIdentity, _ string) error {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin projection retirement: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := lockWriter(ctx, tx, id.Cluster(), generation); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE instance_capacity SET admission_limit=0,retired=true,updated_at=transaction_timestamp()
		WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6`, identityArgs(id)...)
	if err != nil {
		return fmt.Errorf("retire exact capacity: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrInvalidReference
	}
	return tx.Commit(ctx)
}

func (c *Catalog) ActivateDrain(ctx context.Context, generation domain.WriterGeneration, cmd domain.DrainCommand) (domain.DrainIntent, error) {
	intent := cmd.Intent()
	id, exact := intent.Scope().Identity()
	if !exact {
		return domain.DrainIntent{}, domain.ErrInvalidState
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.DrainIntent{}, fmt.Errorf("begin drain: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := lockWriter(ctx, tx, id.Cluster(), generation); err != nil {
		return domain.DrainIntent{}, err
	}
	var ignored int
	if err := tx.QueryRow(ctx, `SELECT admission_limit FROM instance_capacity WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 FOR UPDATE`, identityArgs(id)...).Scan(&ignored); err != nil {
		return domain.DrainIntent{}, fmt.Errorf("lock drain capacity: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE instance_capacity SET admission_limit=0,updated_at=transaction_timestamp() WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6`, identityArgs(id)...); err != nil {
		return domain.DrainIntent{}, err
	}
	deadline, hasDeadline := intent.Deadline()
	var deadlineArg any
	if hasDeadline {
		deadlineArg = deadline
	}
	drainID := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%d/%d", id.PodUID(), intent.Reason(), id.EndpointEpoch(), id.RecoveryEpoch())))
	_, err = tx.Exec(ctx, `INSERT INTO drain_intents (drain_id,cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,scope_kind,state,reason,requested_deadline,writer_generation)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'exact_identity','active',$8,$9,$10)
		ON CONFLICT (cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch) WHERE scope_kind='exact_identity' AND state<>'cleared'
		DO UPDATE SET state='active',reason=EXCLUDED.reason,requested_deadline=EXCLUDED.requested_deadline,writer_generation=EXCLUDED.writer_generation,updated_at=transaction_timestamp()`, hex.EncodeToString(drainID[:]), id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch(), intent.Reason(), deadlineArg, generation)
	if err != nil {
		return domain.DrainIntent{}, fmt.Errorf("upsert drain intent: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.DrainIntent{}, fmt.Errorf("commit drain: %w", err)
	}
	return intent, nil
}
func (c *Catalog) GetDrain(ctx context.Context, id domain.WorkloadIdentity) (domain.DrainIntent, error) {
	var state, reason string
	var deadline *time.Time
	err := c.pool.QueryRow(ctx, `SELECT state,reason,requested_deadline FROM drain_intents
		WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4
		  AND endpoint_epoch=$5 AND recovery_epoch=$6 AND scope_kind='exact_identity'
		  AND state<>'cleared'`, identityArgs(id)...).Scan(&state, &reason, &deadline)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.DrainIntent{}, domain.ErrInvalidReference
	}
	if err != nil {
		return domain.DrainIntent{}, fmt.Errorf("read drain intent: %w", err)
	}
	scope, err := domain.NewInstanceDrainScope(id)
	if err != nil {
		return domain.DrainIntent{}, err
	}
	drainState := domain.DrainActive
	if state == "barrier_pending" {
		drainState = domain.DrainRequested
	}
	params := domain.DrainIntentParams{Scope: scope, State: drainState, Reason: reason}
	if deadline != nil {
		params.Deadline = *deadline
		params.HasDeadline = true
	}
	return domain.NewDrainIntent(params)
}

func (c *Catalog) MarkDrainForced(ctx context.Context, generation domain.WriterGeneration, id domain.WorkloadIdentity, reason string) error {
	if reason == "" || len(reason) > 512 {
		return domain.ErrInvalidState
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockWriter(ctx, tx, id.Cluster(), generation); err != nil {
		return err
	}
	var ignored int
	if err := tx.QueryRow(ctx, `SELECT admission_limit FROM instance_capacity
		WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4
		  AND endpoint_epoch=$5 AND recovery_epoch=$6 FOR UPDATE`, identityArgs(id)...).Scan(&ignored); err != nil {
		return err
	}
	actor := sha256.Sum256([]byte(fmt.Sprint(generation)))
	tag, err := tx.Exec(ctx, `UPDATE drain_intents SET forced_actor_hash=$7,forced_reason=$8,
		updated_at=transaction_timestamp() WHERE cluster_id=$1 AND namespace=$2
		AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6
		AND state<>'cleared'`, append(identityArgs(id), actor[:], reason)...)
	if err != nil {
		return fmt.Errorf("mark drain forced: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrInvalidReference
	}
	return tx.Commit(ctx)
}

func (c *Catalog) InspectActiveUsage(ctx context.Context, scope domain.DrainScope) (domain.ActiveUsage, error) {
	id, exact := scope.Identity()
	if !exact {
		return domain.ActiveUsage{}, domain.ErrInvalidState
	}
	rows, err := c.pool.Query(ctx, `SELECT pod_uid,reserved_slots,orphaned_slots FROM instance_capacity WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 ORDER BY pod_uid`, identityArgs(id)...)
	if err != nil {
		return domain.ActiveUsage{}, fmt.Errorf("inspect active usage: %w", err)
	}
	defer rows.Close()
	var values []domain.UsageByPodParams
	for rows.Next() {
		var p domain.UsageByPodParams
		if err := rows.Scan(&p.PodUID, &p.Reservations, &p.OrphanedSlots); err != nil {
			return domain.ActiveUsage{}, err
		}
		values = append(values, p)
	}
	if err := rows.Err(); err != nil {
		return domain.ActiveUsage{}, err
	}
	return domain.NewActiveUsage(scope, values)
}

func (c *Catalog) BeginRecoveryFence(ctx context.Context, epoch domain.RecoveryEpoch, actor string) error {
	if epoch == 0 || actor == "" {
		return domain.ErrInvalidState
	}
	actorHash := sha256.Sum256([]byte(actor))
	eventID, err := randomID("audit_")
	if err != nil {
		return err
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE system_admission_state SET recovery_epoch=$1,admission_state='fenced',dispatch_state='fenced',fenced_at=transaction_timestamp(),fenced_by_hash=$2,fence_reason='post_restore',changed_at=transaction_timestamp(),changed_by_hash=$2 WHERE recovery_epoch<>$1`, epoch, actorHash[:])
	if err != nil {
		return fmt.Errorf("close recovery fence: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrInvalidState
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events(event_id,event_type,actor_identity_hash,service_identity_hash,target_type,target_hash,reason) VALUES($1,'recovery_fenced',$2,$2,'recovery_epoch',$3,'post_restore')`, eventID, actorHash[:], sha256Bytes(fmt.Sprint(epoch)))
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func sha256Bytes(value string) []byte { sum := sha256.Sum256([]byte(value)); return sum[:] }

func (c *Catalog) ReopenAfterFleetRebuild(ctx context.Context, proof domain.FleetRebuildProof) error {
	if proof.Epoch() == 0 || proof.CurrentPodUIDs().Len() == 0 || proof.OldPodUIDs().Len() != 0 {
		return domain.ErrInvalidState
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var epoch uint64
	var admission, dispatch string
	if err := tx.QueryRow(ctx, `SELECT recovery_epoch,admission_state,dispatch_state FROM system_admission_state FOR UPDATE`).Scan(&epoch, &admission, &dispatch); err != nil {
		return err
	}
	if epoch != uint64(proof.Epoch()) || admission != "fenced" || dispatch != "fenced" {
		return domain.ErrInvalidState
	}
	var incomplete int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM instance_capacity WHERE recovery_epoch<>$1 AND NOT retired`, epoch).Scan(&incomplete); err != nil {
		return err
	}
	if incomplete != 0 {
		return domain.ErrInvalidState
	}
	_, err = tx.Exec(ctx, `UPDATE system_admission_state SET admission_state='open',dispatch_state='open',fenced_at=NULL,fenced_by_hash=NULL,fence_reason=NULL,reopened_at=transaction_timestamp(),changed_at=transaction_timestamp()`)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
