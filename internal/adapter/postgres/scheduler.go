package postgres

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/BizerNotNull/474-Prudentia/internal/registry"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SchedulerStore is a facet of Catalog. It has no tables or transactional
// authority of its own.
type SchedulerStore struct{ *Catalog }

func NewSchedulerStore(pool *pgxpool.Pool, capabilityKey []byte) (*SchedulerStore, error) {
	keyring, err := NewLocalCapabilityKeyring(map[uint32][]byte{1: capabilityKey}, map[uint32][]byte{1: capabilityKey})
	if err != nil || pool == nil {
		return nil, errors.New("invalid scheduler store configuration")
	}
	catalog, err := NewCatalog(pool, keyring)
	if err != nil {
		return nil, err
	}
	return catalog.SchedulerStore(), nil
}

func (s *SchedulerStore) Candidates(ctx context.Context, cmd domain.ScheduleCommand) (domain.CandidateCatalog, error) {
	q, err := registry.NewCandidateQuery(cmd.Model(), cmd.Features(), registry.MaxCandidates)
	if err != nil {
		return domain.CandidateCatalog{}, err
	}
	return s.ListCandidateSnapshots(ctx, q)
}

func (s *SchedulerStore) LookupReservation(ctx context.Context, cmd domain.ScheduleCommand) (domain.Reservation, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.Reservation{}, false, fmt.Errorf("begin reservation lookup: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := lockSystemRows(ctx, tx, false); err != nil {
		return domain.Reservation{}, false, err
	}
	res, found, err := s.lookupRequest(ctx, tx, cmd, sha256.Sum256([]byte(cmd.Tenant())))
	if err != nil {
		return domain.Reservation{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Reservation{}, false, fmt.Errorf("commit reservation lookup: %w", err)
	}
	return res, found, nil
}

func (s *SchedulerStore) TryReserve(ctx context.Context, cmd domain.ScheduleCommand, id domain.WorkloadIdentity) (domain.Reservation, error) {
	tenant := sha256.Sum256([]byte(cmd.Tenant()))
	attempt := sha256.Sum256([]byte(cmd.AttemptID()))
	digest, err := digestWriteCandidate(cmd)
	if err != nil {
		return domain.Reservation{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.Reservation{}, fmt.Errorf("begin reservation: %w", err)
	}
	defer tx.Rollback(ctx)
	versions, err := lockAdmissionRow(ctx, tx, id.Cluster(), true)
	if err != nil {
		return domain.Reservation{}, err
	}
	// Retained-version lookup is deliberately before current-write validation.
	if existing, found, err := s.lookupRequest(ctx, tx, cmd, tenant); err != nil || found {
		return existing, err
	}
	if err := requireRetainedCandidates(ctx, tx, id.Cluster(), cmd); err != nil {
		return domain.Reservation{}, err
	}
	if (cmd.HasIdempotencyKey() && versions.lookup != cmd.LookupWriteVersion()) ||
		versions.digest != cmd.DigestWriteVersion() {
		return domain.Reservation{}, domain.ErrInvalidState
	}
	if !s.keyring.CanRead(versions.kek, versions.comparison) {
		return domain.Reservation{}, domain.ErrInvalidState
	}

	var requestID, grantID string
	var generation uint64 = 1
	var rerank bool
	err = tx.QueryRow(ctx, `SELECT request_id,current_generation FROM request_records WHERE request_id=$1 FOR UPDATE`, cmd.RequestID()).Scan(&requestID, &generation)
	if err == nil {
		var owner []byte
		var stage string
		if err := tx.QueryRow(ctx, `SELECT owner_attempt_hash,stage FROM request_records WHERE request_id=$1`, requestID).Scan(&owner, &stage); err != nil {
			return domain.Reservation{}, err
		}
		if !equalBytes(owner, attempt[:]) || stage != "rerank_pending" {
			return domain.Reservation{}, domain.ErrInvalidState
		}
		generation++
		rerank = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Reservation{}, fmt.Errorf("lock request: %w", err)
	}

	if err := lockTenantCounter(ctx, tx, tenant, int(cmd.SlotCost()), !rerank); err != nil {
		return domain.Reservation{}, err
	}
	if rerank {
		if err := tx.QueryRow(ctx, `SELECT grant_id FROM admission_grants WHERE request_id=$1 AND state='retained_rerank' FOR UPDATE`, requestID).Scan(&grantID); err != nil {
			return domain.Reservation{}, domain.ErrInvalidState
		}
	} else {
		requestID = cmd.RequestID()
		grantID, err = randomID("grant_")
		if err != nil {
			return domain.Reservation{}, err
		}
	}
	if err := lockCapacityAndCatalog(ctx, tx, id, int(cmd.SlotCost()), cmd.Model()); err != nil {

		return domain.Reservation{}, err
	}

	reservationID, err := randomID("res_")
	if err != nil {
		return domain.Reservation{}, err
	}
	capability := make([]byte, 32)
	if _, err := rand.Read(capability); err != nil {
		return domain.Reservation{}, err
	}
	aad := CapabilityAAD{ReservationID: reservationID, Generation: generation, OwnerAttempt: attempt, Identity: id}
	sealed, err := s.keyring.Seal(ctx, capability, aad, versions.kek, versions.comparison)
	if err != nil {
		return domain.Reservation{}, err
	}
	classificationMS := cmd.ExecutionBudget().Milliseconds()
	if classificationMS < 1 {
		return domain.Reservation{}, domain.ErrInvalidState
	}
	if rerank {
		if _, err := tx.Exec(ctx, `UPDATE reservations SET is_current=false,updated_at=transaction_timestamp() WHERE request_id=$1 AND is_current`, requestID); err != nil {
			return domain.Reservation{}, err
		}
		if _, err := tx.Exec(ctx, `UPDATE request_records SET current_generation=$2,stage='reserved',updated_at=transaction_timestamp() WHERE request_id=$1`, requestID, generation); err != nil {
			return domain.Reservation{}, err
		}
	} else {
		var lookupVersion any
		var lookupHMAC any
		if cmd.HasIdempotencyKey() {
			candidate, e := lookupWriteCandidate(cmd)
			if e != nil {
				return domain.Reservation{}, e
			}
			value := candidate.Value()
			lookupVersion, lookupHMAC = candidate.Version(), value[:]
		}
		dv := digest.Value()
		_, err = tx.Exec(ctx, `INSERT INTO request_records(request_id,tenant_hash,idempotency_lookup_version,idempotency_lookup_hmac,digest_version,request_digest,owner_attempt_hash,current_generation,stage,execution_deadline,classification_after,mutation_retry_deadline) VALUES($1,$2,$3,$4,$5,$6,$7,1,'reserved',transaction_timestamp()+($8*interval '1 millisecond'),transaction_timestamp()+($8*interval '1 millisecond'),transaction_timestamp()+($9*interval '1 millisecond'))`, requestID, tenant[:], lookupVersion, lookupHMAC, digest.Version(), dv[:], attempt[:], classificationMS, classificationMS+300000)
		if err != nil {
			return domain.Reservation{}, fmt.Errorf("insert request record: %w", err)
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO reservations(reservation_id,request_id,request_generation,owner_attempt_hash,cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,slot_cost,state,capability_algorithm,capability_kek_version,capability_wrapped_data_key,capability_nonce,capability_ciphertext,capability_comparison_version,capability_comparison_hash,execution_deadline,classification_after,capability_retry_deadline) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'reserved',$12,$13,$14,$15,$16,$17,$18,transaction_timestamp()+($19*interval '1 millisecond'),transaction_timestamp()+($19*interval '1 millisecond'),transaction_timestamp()+($20*interval '1 millisecond'))`, reservationID, requestID, generation, attempt[:], id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch(), cmd.SlotCost(), sealed.Algorithm, sealed.KEKVersion, sealed.WrappedDataKey, sealed.Nonce, sealed.Ciphertext, sealed.ComparisonVersion, sealed.ComparisonHash[:], classificationMS, classificationMS+300000)
	if err != nil {
		return domain.Reservation{}, fmt.Errorf("insert reservation: %w", err)
	}
	if rerank {
		if _, err := tx.Exec(ctx, `UPDATE admission_grants SET reservation_id=$2,state='active_reserved',updated_at=transaction_timestamp() WHERE grant_id=$1 AND state='retained_rerank'`, grantID, reservationID); err != nil {
			return domain.Reservation{}, err
		}
	}
	if !rerank {
		_, err = tx.Exec(ctx, `INSERT INTO admission_grants(grant_id,request_id,reservation_id,tenant_hash,slot_cost,state,execution_deadline,classification_after) VALUES($1,$2,$3,$4,$5,'active_reserved',transaction_timestamp()+($6*interval '1 millisecond'),transaction_timestamp()+($6*interval '1 millisecond'))`, grantID, requestID, reservationID, tenant[:], cmd.SlotCost(), classificationMS)
		if err != nil {
			return domain.Reservation{}, fmt.Errorf("insert admission grant: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Reservation{}, fmt.Errorf("commit reservation: %w", err)
	}
	ref, err := domain.NewReservationRef(reservationID, generation, capability)
	if err != nil {
		return domain.Reservation{}, err
	}
	return domain.NewReservation(ref), nil
}

func lockTenantCounter(ctx context.Context, tx pgx.Tx, tenant [32]byte, cost int, increment bool) error {
	var limit, active, orphaned int
	if err := tx.QueryRow(ctx, `SELECT grant_limit,active_grants,orphaned_grants FROM tenant_counters WHERE tenant_hash=$1 FOR UPDATE`, tenant[:]).Scan(&limit, &active, &orphaned); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrNoCapacity
		}
		return err
	}
	if !increment {
		return nil
	}
	if cost > limit-active-orphaned {
		return domain.ErrNoCapacity
	}
	tag, err := tx.Exec(ctx, `UPDATE tenant_counters SET active_grants=active_grants+$2,version=version+1 WHERE tenant_hash=$1`, tenant[:], cost)
	if err != nil || tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	return nil
}

type admissionVersions struct{ lookup, digest, kek, comparison uint32 }

func lockSystemRows(ctx context.Context, tx pgx.Tx, exclusive bool) error {
	clause := "FOR SHARE"
	if exclusive {
		clause = "FOR UPDATE"
	}
	_, err := tx.Exec(ctx, `SELECT cluster_id FROM system_admission_state ORDER BY cluster_id `+clause)
	if err != nil {
		return fmt.Errorf("lock admission state: %w", err)
	}
	return nil
}
func lockAdmissionRow(ctx context.Context, tx pgx.Tx, cluster string, requireAdmission bool) (admissionVersions, error) {
	var v admissionVersions
	var admission, dispatch string
	err := tx.QueryRow(ctx, `SELECT admission_state,dispatch_state,lookup_write_version,digest_write_version,capability_kek_write_version,capability_comparison_write_version FROM system_admission_state WHERE cluster_id=$1 FOR UPDATE`, cluster).Scan(&admission, &dispatch, &v.lookup, &v.digest, &v.kek, &v.comparison)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, domain.ErrInvalidState
	}
	if err != nil {
		return v, fmt.Errorf("lock recovery fence: %w", err)
	}
	if (requireAdmission && admission != "open") || dispatch != "open" {
		return v, domain.ErrInvalidState
	}
	return v, nil
}

func requireRetainedCandidates(ctx context.Context, tx pgx.Tx, cluster string, cmd domain.ScheduleCommand) error {
	lookups := make(map[uint32]struct{}, len(cmd.IdempotencyCandidates()))
	for _, candidate := range cmd.IdempotencyCandidates() {
		lookups[candidate.Version()] = struct{}{}
	}
	digests := make(map[uint32]struct{}, len(cmd.DigestCandidates()))
	for _, candidate := range cmd.DigestCandidates() {
		digests[candidate.Version()] = struct{}{}
	}
	rows, err := tx.Query(ctx, `SELECT version FROM system_digest_read_versions WHERE cluster_id=$1 ORDER BY version FOR SHARE`, cluster)
	if err != nil {
		return fmt.Errorf("read retained digest versions: %w", err)
	}
	for rows.Next() {
		var version uint32
		if err := rows.Scan(&version); err != nil {
			rows.Close()
			return err
		}
		if _, ok := digests[version]; !ok {
			rows.Close()
			return domain.ErrInvalidState
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if !cmd.HasIdempotencyKey() {
		return nil
	}
	rows, err = tx.Query(ctx, `SELECT version FROM system_lookup_read_versions WHERE cluster_id=$1 ORDER BY version FOR SHARE`, cluster)
	if err != nil {
		return fmt.Errorf("read retained lookup versions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var version uint32
		if err := rows.Scan(&version); err != nil {
			return err
		}
		if _, ok := lookups[version]; !ok {
			return domain.ErrInvalidState
		}
	}
	return rows.Err()
}

func lockCapacityAndCatalog(ctx context.Context, tx pgx.Tx, id domain.WorkloadIdentity, cost int, model string) error {
	var physical, limit, reserved, orphaned int
	var retired bool
	err := tx.QueryRow(ctx, `SELECT physical_slots,admission_limit,reserved_slots,orphaned_slots,retired FROM instance_capacity WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 FOR UPDATE`, id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch()).Scan(&physical, &limit, &reserved, &orphaned, &retired)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNoCapacity
	}
	if err != nil {
		return err
	}
	if retired || cost > limit-reserved-orphaned || reserved+orphaned+cost > physical {
		return domain.ErrNoCapacity
	}
	draining, err := lockApplicableDrains(ctx, tx, id)
	if err != nil {
		return err
	}
	if draining {
		return domain.ErrNoCapacity
	}
	var health string
	if err := tx.QueryRow(ctx, `SELECT health FROM instance_projections WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 FOR UPDATE`, id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch()).Scan(&health); err != nil {
		return domain.ErrNoCapacity
	}
	if health != "healthy" {
		return domain.ErrNoCapacity
	}
	if _, err := tx.Exec(ctx, `UPDATE instance_capacity SET reserved_slots=reserved_slots+$7,updated_at=transaction_timestamp() WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6`, id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch(), cost); err != nil {
		return err
	}
	return nil
}

func lockApplicableDrains(ctx context.Context, tx pgx.Tx, id domain.WorkloadIdentity) (bool, error) {
	rows, err := tx.Query(ctx, `SELECT drain_id FROM drain_intents
		WHERE cluster_id=$1 AND state IN('active','barrier_pending','barrier_observed')
		  AND ((scope_kind='exact_identity' AND namespace=$2 AND logical_engine=$3
		        AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6)
		       OR scope_kind='workload')
		ORDER BY drain_id FOR UPDATE`,
		id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(),
		id.EndpointEpoch(), id.RecoveryEpoch())
	if err != nil {
		return false, err
	}
	defer rows.Close()
	draining := rows.Next()
	if err := rows.Err(); err != nil {
		return false, err
	}
	return draining, nil
}

func (s *SchedulerStore) lookupRequest(ctx context.Context, tx pgx.Tx, cmd domain.ScheduleCommand, tenant [32]byte) (domain.Reservation, bool, error) {
	var requestID, stage string
	var storedDigest []byte
	var digestVersion uint32
	var owner []byte
	if cmd.HasIdempotencyKey() {
		matches := 0
		for _, candidate := range cmd.IdempotencyCandidates() {
			v := candidate.Value()
			var rid, st string
			var d []byte
			var dv uint32
			var o []byte
			err := tx.QueryRow(ctx, `SELECT request_id,stage,digest_version,request_digest,owner_attempt_hash FROM request_records WHERE tenant_hash=$1 AND idempotency_lookup_version=$2 AND idempotency_lookup_hmac=$3 FOR UPDATE`, tenant[:], candidate.Version(), v[:]).Scan(&rid, &st, &dv, &d, &o)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return domain.Reservation{}, false, err
			}
			matches++
			requestID, stage, digestVersion, storedDigest, owner = rid, st, dv, d, o
		}
		if matches == 0 {
			return domain.Reservation{}, false, nil
		}
		if matches != 1 {
			return domain.Reservation{}, false, domain.ErrInvalidState
		}
	} else {
		err := tx.QueryRow(ctx, `SELECT request_id,stage,digest_version,request_digest,owner_attempt_hash FROM request_records WHERE request_id=$1 FOR UPDATE`, cmd.RequestID()).Scan(&requestID, &stage, &digestVersion, &storedDigest, &owner)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Reservation{}, false, nil
		}
		if err != nil {
			return domain.Reservation{}, false, err
		}
	}
	d, ok := digestCandidate(cmd, digestVersion)
	if !ok {
		return domain.Reservation{}, false, domain.ErrInvalidState
	}
	dv := d.Value()
	if !equalBytes(dv[:], storedDigest) {
		return domain.Reservation{}, false, domain.ErrIdempotencyConflict
	}
	attempt := sha256.Sum256([]byte(cmd.AttemptID()))
	if !equalBytes(owner, attempt[:]) {
		if stage == "reserved" || stage == "dispatch_authorized" || stage == "streaming" {
			return domain.Reservation{}, false, domain.ErrRequestInProgress
		}
		return domain.Reservation{}, false, domain.ErrRequestNotReplayable
	}
	if stage == "rerank_pending" {
		return domain.Reservation{}, false, nil
	}
	if stage == "terminal" {
		return domain.Reservation{}, false, domain.ErrRequestNotReplayable
	}
	var row reservationRow
	err := tx.QueryRow(ctx, reservationSelect+` WHERE r.request_id=$1 AND r.is_current FOR UPDATE OF r`, requestID).Scan(row.scan()...)
	if err != nil {
		return domain.Reservation{}, false, err
	}
	plain, err := s.keyring.Open(ctx, row.sealed(), row.aad())
	if err != nil {
		return domain.Reservation{}, false, err
	}
	defer clear(plain)
	ref, err := domain.NewReservationRef(row.reservationID, row.generation, plain)
	if err != nil {
		return domain.Reservation{}, false, err
	}
	return domain.NewReservation(ref), true, nil
}

const reservationSelect = `SELECT r.reservation_id,r.request_id,r.request_generation,r.owner_attempt_hash,r.state,r.cluster_id,r.namespace,r.logical_engine,r.pod_uid,r.endpoint_epoch,r.recovery_epoch,r.slot_cost,r.capability_algorithm,r.capability_kek_version,r.capability_wrapped_data_key,r.capability_nonce,r.capability_ciphertext,r.capability_comparison_version,r.capability_comparison_hash,r.execution_deadline>transaction_timestamp(),q.tenant_hash FROM reservations r JOIN request_records q USING(request_id)`

type reservationRow struct {
	reservationID, requestID, state, cluster, namespace, engine, podUID, algorithm string
	generation, endpointEpoch, recoveryEpoch                                       uint64
	slotCost                                                                       int
	owner, wrapped, nonce, ciphertext, comparisonHash, tenant                      []byte
	kek, comparison                                                                uint32
	beforeDeadline                                                                 bool
}

func (r *reservationRow) scan() []any {
	return []any{&r.reservationID, &r.requestID, &r.generation, &r.owner, &r.state, &r.cluster, &r.namespace, &r.engine, &r.podUID, &r.endpointEpoch, &r.recoveryEpoch, &r.slotCost, &r.algorithm, &r.kek, &r.wrapped, &r.nonce, &r.ciphertext, &r.comparison, &r.comparisonHash, &r.beforeDeadline, &r.tenant}
}
func (r reservationRow) identity() domain.WorkloadIdentity {
	id, _ := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: r.cluster, Namespace: r.namespace, LogicalEngine: r.engine, PodUID: r.podUID, EndpointEpoch: r.endpointEpoch, RecoveryEpoch: r.recoveryEpoch})
	return id
}
func (r reservationRow) aad() CapabilityAAD {
	var o [32]byte
	copy(o[:], r.owner)
	return CapabilityAAD{ReservationID: r.reservationID, Generation: r.generation, OwnerAttempt: o, Identity: r.identity()}
}
func (r reservationRow) sealed() SealedCapability {
	var h [32]byte
	copy(h[:], r.comparisonHash)
	return SealedCapability{Algorithm: r.algorithm, KEKVersion: r.kek, ComparisonVersion: r.comparison, WrappedDataKey: r.wrapped, Nonce: r.nonce, Ciphertext: r.ciphertext, ComparisonHash: h}
}
func (s *SchedulerStore) lockRef(ctx context.Context, tx pgx.Tx, ref domain.ReservationRef) (reservationRow, error) {
	var r reservationRow
	err := tx.QueryRow(ctx, reservationSelect+` WHERE r.reservation_id=$1 FOR UPDATE OF r`, ref.ID()).Scan(r.scan()...)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, domain.ErrInvalidReference
	}
	if err != nil {
		return r, err
	}
	if r.generation != ref.Generation() {
		return r, domain.ErrInvalidReference
	}
	ok, err := s.keyring.Matches(ctx, ref.Capability(), r.sealed(), r.aad())
	if err != nil || !ok {
		return r, domain.ErrInvalidReference
	}
	return r, nil
}

func (s *SchedulerStore) PrepareDispatch(ctx context.Context, ref domain.ReservationRef) (domain.DispatchTarget, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.DispatchTarget{}, err
	}
	defer tx.Rollback(ctx)
	// Lock system, request, tenant/grant, exact capacity, exact drain, projection, reservation.
	var cluster string
	if err := tx.QueryRow(ctx, `SELECT cluster_id FROM reservations WHERE reservation_id=$1`, ref.ID()).Scan(&cluster); err != nil {
		return domain.DispatchTarget{}, domain.ErrInvalidReference
	}
	if _, err := lockAdmissionRow(ctx, tx, cluster, false); err != nil {
		return domain.DispatchTarget{}, err
	}
	var requestID string
	if err := tx.QueryRow(ctx, `SELECT request_id FROM reservations WHERE reservation_id=$1`, ref.ID()).Scan(&requestID); err != nil {
		return domain.DispatchTarget{}, domain.ErrInvalidReference
	}
	if _, err := tx.Exec(ctx, `SELECT request_id FROM request_records WHERE request_id=$1 FOR UPDATE`, requestID); err != nil {
		return domain.DispatchTarget{}, err
	}
	r, err := s.lockRef(ctx, tx, ref)
	if err != nil {
		return domain.DispatchTarget{}, err
	}
	if (r.state != "reserved" && r.state != "dispatch_authorized") || !r.beforeDeadline {
		return domain.DispatchTarget{}, domain.ErrInvalidState
	}
	id := r.identity()
	var retired bool
	if err := tx.QueryRow(ctx, `SELECT retired FROM instance_capacity WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 FOR UPDATE`, id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch()).Scan(&retired); err != nil || retired {
		return domain.DispatchTarget{}, domain.ErrStaleTarget
	}
	drain, err := lockApplicableDrains(ctx, tx, id)
	if err != nil {
		return domain.DispatchTarget{}, err
	}
	if drain {
		return domain.DispatchTarget{}, domain.ErrStaleTarget
	}
	var endpoint, health string
	if err := tx.QueryRow(ctx, `SELECT normalized_proxy_endpoint,health FROM instance_projections WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 FOR UPDATE`, id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch()).Scan(&endpoint, &health); err != nil || health != "healthy" {
		return domain.DispatchTarget{}, domain.ErrStaleTarget
	}
	if r.state == "reserved" {
		if _, err = tx.Exec(ctx, `UPDATE reservations SET state='dispatch_authorized',updated_at=transaction_timestamp() WHERE reservation_id=$1`, r.reservationID); err != nil {
			return domain.DispatchTarget{}, err
		}
		if _, err = tx.Exec(ctx, `UPDATE request_records SET stage='dispatch_authorized',updated_at=transaction_timestamp() WHERE request_id=$1`, r.requestID); err != nil {
			return domain.DispatchTarget{}, err
		}
	}
	target, err := domain.NewDispatchTarget(endpoint, id)
	if err != nil {
		return domain.DispatchTarget{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.DispatchTarget{}, err
	}
	return target, nil
}

func (s *SchedulerStore) AbandonBeforeDispatch(ctx context.Context, ref domain.ReservationRef, reason domain.RerankReason) error {
	if reason != domain.RerankStaleTarget {
		return domain.ErrInvalidState
	}
	return s.transitionNeverDispatched(ctx, ref, "abandoned_rerank", "rerank_pending", "retained_rerank", "", false)
}
func (s *SchedulerStore) GiveUpBeforeDispatch(ctx context.Context, ref domain.ReservationRef, reason domain.GiveUpReason) error {
	if reason < domain.GiveUpCanceled || reason > domain.GiveUpReranksExhausted {
		return domain.ErrInvalidState
	}
	return s.transitionNeverDispatched(ctx, ref, "released", "terminal", "released", fmt.Sprintf("client_give_up:%d", reason), true)
}
func (s *SchedulerStore) transitionNeverDispatched(ctx context.Context, ref domain.ReservationRef, resState, reqStage, grantState, proof string, terminal bool) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockSystemRows(ctx, tx, true); err != nil {
		return err
	}
	r, err := s.lockRef(ctx, tx, ref)
	if err != nil {
		return err
	}
	if r.state == resState {
		if terminal {
			expected := sha256.Sum256([]byte(proof))
			var kind string
			var stored []byte
			if err := tx.QueryRow(ctx, `SELECT terminal_proof_kind,terminal_proof_hash
				FROM reservations WHERE reservation_id=$1`, r.reservationID).Scan(&kind, &stored); err != nil ||
				kind != "client_give_up" || !equalBytes(stored, expected[:]) {
				return domain.ErrInvalidState
			}
		}
		return tx.Commit(ctx)
	}
	if r.state != "reserved" && !(terminal && r.state == "abandoned_rerank") {
		return domain.ErrInvalidState
	}
	var active int
	if err := tx.QueryRow(ctx, `SELECT active_grants FROM tenant_counters WHERE tenant_hash=$1 FOR UPDATE`, r.tenant).Scan(&active); err != nil || active < r.slotCost {
		return domain.ErrInvalidState
	}
	var gs string
	if err := tx.QueryRow(ctx, `SELECT state FROM admission_grants WHERE request_id=$1 FOR UPDATE`, r.requestID).Scan(&gs); err != nil {
		return err
	}
	expected := "active_reserved"
	if r.state == "abandoned_rerank" {
		expected = "retained_rerank"
	}
	if gs != expected {
		return domain.ErrInvalidState
	}
	if r.state == "reserved" {
		tag, err := tx.Exec(ctx, `UPDATE instance_capacity SET reserved_slots=reserved_slots-$7,updated_at=transaction_timestamp() WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 AND reserved_slots>=$7`, r.cluster, r.namespace, r.engine, r.podUID, r.endpointEpoch, r.recoveryEpoch, r.slotCost)
		if err != nil || tag.RowsAffected() != 1 {
			return domain.ErrInvalidState
		}
	}
	var reservationID any = r.reservationID
	if grantState == "retained_rerank" {
		reservationID = nil
	}
	_, err = tx.Exec(ctx, `UPDATE admission_grants SET state=$2,reservation_id=$3,updated_at=transaction_timestamp() WHERE request_id=$1`, r.requestID, grantState, reservationID)
	if err != nil {
		return err
	}
	if terminal {
		_, err = tx.Exec(ctx, `UPDATE tenant_counters SET active_grants=active_grants-$2,version=version+1 WHERE tenant_hash=$1`, r.tenant, r.slotCost)
		if err != nil {
			return err
		}
		proofHash := sha256.Sum256([]byte(proof))
		_, err = tx.Exec(ctx, `UPDATE request_records SET stage='terminal',outcome='given_up',terminal_at=transaction_timestamp(),updated_at=transaction_timestamp() WHERE request_id=$1`, r.requestID)
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE reservations SET state='released',is_current=false,terminal_proof_kind='client_give_up',terminal_proof_hash=$2,terminal_at=transaction_timestamp(),updated_at=transaction_timestamp() WHERE reservation_id=$1`, r.reservationID, proofHash[:])
		}
	} else {
		_, err = tx.Exec(ctx, `UPDATE request_records SET stage=$2,updated_at=transaction_timestamp() WHERE request_id=$1`, r.requestID, reqStage)
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE reservations SET state=$2,updated_at=transaction_timestamp() WHERE reservation_id=$1`, r.reservationID, resState)
		}
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *SchedulerStore) Finalize(ctx context.Context, ref domain.ReservationRef, proof domain.TerminalProof) error {
	var kind string
	switch proof {
	case domain.TerminalProofProviderFinish:
		kind = "provider_finish"
	case domain.TerminalProofCompleteNonStreaming:
		kind = "complete_response"
	case domain.TerminalProofNotSent:
		kind = "not_sent"
	default:
		return domain.ErrInvalidState
	}
	return s.release(ctx, ref, kind)
}
func (s *SchedulerStore) release(ctx context.Context, ref domain.ReservationRef, kind string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockSystemRows(ctx, tx, true); err != nil {
		return err
	}
	r, err := s.lockRef(ctx, tx, ref)
	if err != nil {
		return err
	}
	proofHash := sha256.Sum256([]byte(kind))
	if r.state == "released" {
		var k string
		var h []byte
		if err := tx.QueryRow(ctx, `SELECT terminal_proof_kind,terminal_proof_hash FROM reservations WHERE reservation_id=$1`, r.reservationID).Scan(&k, &h); err != nil || k != kind || !equalBytes(h, proofHash[:]) {
			return domain.ErrInvalidState
		}
		return tx.Commit(ctx)
	}
	if (kind == "not_sent" && r.state != "reserved") || (kind != "not_sent" && r.state != "dispatch_authorized" && r.state != "streaming") {
		return domain.ErrInvalidState
	}
	return s.finishReleaseTx(ctx, tx, r, kind, proofHash)
}
func (s *SchedulerStore) finishReleaseTx(ctx context.Context, tx pgx.Tx, r reservationRow, kind string, proofHash [32]byte) error {
	if _, err := tx.Exec(ctx, `SELECT tenant_hash FROM tenant_counters WHERE tenant_hash=$1 FOR UPDATE`, r.tenant); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT grant_id FROM admission_grants WHERE request_id=$1 FOR UPDATE`, r.requestID); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE instance_capacity SET reserved_slots=reserved_slots-$7,updated_at=transaction_timestamp() WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 AND reserved_slots>=$7`, r.cluster, r.namespace, r.engine, r.podUID, r.endpointEpoch, r.recoveryEpoch, r.slotCost)
	if err != nil || tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	if _, err = tx.Exec(ctx, `UPDATE tenant_counters SET active_grants=active_grants-$2,version=version+1 WHERE tenant_hash=$1`, r.tenant, r.slotCost); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE admission_grants SET state='released',updated_at=transaction_timestamp() WHERE request_id=$1`, r.requestID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE reservations SET state='released',is_current=false,terminal_proof_kind=$2,terminal_proof_hash=$3,terminal_at=transaction_timestamp(),updated_at=transaction_timestamp() WHERE reservation_id=$1`, r.reservationID, kind, proofHash[:]); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE request_records SET stage='terminal',outcome='succeeded',terminal_at=transaction_timestamp(),updated_at=transaction_timestamp() WHERE request_id=$1`, r.requestID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *SchedulerStore) MarkAmbiguous(ctx context.Context, ref domain.ReservationRef, cause domain.AmbiguousCause) error {
	var c string
	switch cause {
	case domain.AmbiguousTransport:
		c = "ambiguous_transport"
	case domain.AmbiguousCanceled:
		c = "ambiguous_canceled"
	case domain.AmbiguousProtocol:
		c = "ambiguous_protocol"
	default:
		return domain.ErrInvalidState
	}
	return s.orphan(ctx, ref, c)
}
func (s *SchedulerStore) orphan(ctx context.Context, ref domain.ReservationRef, cause string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockSystemRows(ctx, tx, true); err != nil {
		return err
	}
	r, err := s.lockRef(ctx, tx, ref)
	if err != nil {
		return err
	}
	if r.state == "orphaned" {
		var c string
		if err := tx.QueryRow(ctx, `SELECT cause FROM orphaned_capacity_debts WHERE reservation_id=$1`, r.reservationID).Scan(&c); err != nil || c != cause {
			return domain.ErrInvalidState
		}
		return tx.Commit(ctx)
	}
	if r.state != "dispatch_authorized" && r.state != "streaming" {
		return domain.ErrInvalidState
	}
	if _, err := tx.Exec(ctx, `SELECT tenant_hash FROM tenant_counters WHERE tenant_hash=$1 FOR UPDATE`, r.tenant); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT grant_id FROM admission_grants WHERE request_id=$1 FOR UPDATE`, r.requestID); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE instance_capacity SET reserved_slots=reserved_slots-$7,orphaned_slots=orphaned_slots+$7,updated_at=transaction_timestamp() WHERE cluster_id=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 AND reserved_slots>=$7`, r.cluster, r.namespace, r.engine, r.podUID, r.endpointEpoch, r.recoveryEpoch, r.slotCost)
	if err != nil || tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	ph := sha256.Sum256([]byte(cause))
	debtID := "debt_" + r.reservationID
	if _, err = tx.Exec(ctx, `UPDATE tenant_counters SET active_grants=active_grants-$2,orphaned_grants=orphaned_grants+$2,version=version+1 WHERE tenant_hash=$1`, r.tenant, r.slotCost); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE admission_grants SET state='orphaned',updated_at=transaction_timestamp() WHERE request_id=$1`, r.requestID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE reservations SET state='orphaned',is_current=false,terminal_proof_kind='ambiguity',terminal_proof_hash=$2,terminal_at=transaction_timestamp(),updated_at=transaction_timestamp() WHERE reservation_id=$1`, r.reservationID, ph[:]); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE request_records SET stage='terminal',outcome='ambiguous',terminal_at=transaction_timestamp(),updated_at=transaction_timestamp() WHERE request_id=$1`, r.requestID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO orphaned_capacity_debts(debt_id,reservation_id,request_id,tenant_hash,cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,slot_cost,cause,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'active')`, debtID, r.reservationID, r.requestID, r.tenant, r.cluster, r.namespace, r.engine, r.podUID, r.endpointEpoch, r.recoveryEpoch, r.slotCost, cause); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *SchedulerStore) ClassifyExpired(ctx context.Context, limit int) (int, error) {
	result, err := s.SweepReservationStates(ctx, limit)
	return int(result.GivenUp + result.ConvertedToDebt), err
}

func lookupWriteCandidate(cmd domain.ScheduleCommand) (domain.IdempotencyLookupCandidate, error) {
	for _, c := range cmd.IdempotencyCandidates() {
		if c.Version() == cmd.LookupWriteVersion() {
			return c, nil
		}
	}
	return domain.IdempotencyLookupCandidate{}, domain.ErrInvalidState
}
func digestWriteCandidate(cmd domain.ScheduleCommand) (domain.RequestDigestCandidate, error) {
	c, ok := digestCandidate(cmd, cmd.DigestWriteVersion())
	if !ok {
		return domain.RequestDigestCandidate{}, domain.ErrInvalidState
	}
	return c, nil
}
func digestCandidate(cmd domain.ScheduleCommand, version uint32) (domain.RequestDigestCandidate, bool) {
	for _, c := range cmd.DigestCandidates() {
		if c.Version() == version {
			return c, true
		}
	}
	return domain.RequestDigestCandidate{}, false
}
func randomID(prefix string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(b[:]), nil
}
func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var d byte
	for i := range a {
		d |= a[i] ^ b[i]
	}
	return d == 0
}
