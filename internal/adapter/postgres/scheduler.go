package postgres

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/BizerNotNull/474-Prudentia/internal/registry"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SchedulerStore struct {
	*Catalog
}

func NewSchedulerStore(pool *pgxpool.Pool, capabilityKey []byte) (*SchedulerStore, error) {
	keyring, err := NewLocalCapabilityKeyring(
		map[uint32][]byte{1: capabilityKey},
		map[uint32][]byte{1: capabilityKey},
	)
	if err != nil || pool == nil {
		return nil, errors.New("invalid scheduler store configuration")
	}
	catalog, err := NewCatalog(pool, keyring)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(capabilityKey)
	if err != nil {
		return nil, fmt.Errorf("create capability cipher: %w", err)
	}
	catalog.aead, err = cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create capability AEAD: %w", err)
	}
	return catalog.SchedulerStore(), nil
}

func (s *SchedulerStore) Candidates(ctx context.Context, cmd domain.ScheduleCommand) (domain.CandidateCatalog, error) {
	query, err := registry.NewCandidateQuery(cmd.Model(), cmd.Features(), registry.MaxCandidates)
	if err != nil {
		return domain.CandidateCatalog{}, err
	}
	return s.ListCandidateSnapshots(ctx, query)
}

func (s *SchedulerStore) LookupReservation(ctx context.Context, cmd domain.ScheduleCommand) (domain.Reservation, bool, error) {
	commandHash := hashCommand(cmd)
	tenantHash := sha256.Sum256([]byte(cmd.Tenant()))
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return domain.Reservation{}, false, fmt.Errorf("begin idempotency lookup: %w", err)
	}
	defer tx.Rollback(ctx)
	if cmd.HasIdempotencyKey() {
		if err := lockAndValidateCryptoVersions(ctx, tx, cmd, tenantHash); err != nil {
			return domain.Reservation{}, false, err
		}
	}
	reservation, found, err := s.existingReservation(ctx, tx, cmd, commandHash, tenantHash)
	if err != nil {
		return domain.Reservation{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Reservation{}, false, fmt.Errorf("commit idempotency lookup: %w", err)
	}
	return reservation, found, nil
}

func (s *SchedulerStore) TryReserve(ctx context.Context, cmd domain.ScheduleCommand, identity domain.WorkloadIdentity) (domain.Reservation, error) {
	commandHash := hashCommand(cmd)
	tenantHash := sha256.Sum256([]byte(cmd.Tenant()))
	// Read Committed is deliberate: the request row, tenant counter/grant, and exact backend row
	// are locked in that order, so every accounting invariant has a single serialization point.
	// This prevents write skew and lost updates without surfacing serialization failures to losers.
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.Reservation{}, fmt.Errorf("begin reservation: %w", err)
	}
	defer tx.Rollback(ctx)

	if cmd.HasIdempotencyKey() {
		if err := lockAndValidateCryptoVersions(ctx, tx, cmd, tenantHash); err != nil {
			return domain.Reservation{}, err
		}
	}
	reservation, found, err := s.existingReservation(ctx, tx, cmd, commandHash, tenantHash)
	if err != nil || found {
		return reservation, err
	}
	var lookupWrite domain.IdempotencyLookupCandidate
	var digestWrite domain.RequestDigestCandidate
	if cmd.HasIdempotencyKey() {
		lookupWrite, err = lookupWriteCandidate(cmd)
		if err != nil {
			return domain.Reservation{}, err
		}
		digestWrite, err = digestWriteCandidate(cmd)
		if err != nil {
			return domain.Reservation{}, err
		}
	}

	var reservationID string
	var generation uint64
	err = tx.QueryRow(ctx, `SELECT reservation_id, request_generation
		FROM scheduler_reservations
		WHERE request_id=$1 AND tenant_hash=$2 AND command_hash=$3 AND attempt_id=$4
		  AND state='abandoned_rerank'
		FOR UPDATE`, cmd.RequestID(), tenantHash[:], commandHash[:], cmd.AttemptID()).Scan(&reservationID, &generation)
	rerank := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		reservationID, err = randomID("res_")
		generation = 1
	} else if err != nil {
		return domain.Reservation{}, fmt.Errorf("lock rerank reservation: %w", err)
	} else if generation >= uint64(math.MaxInt64) {
		return domain.Reservation{}, domain.ErrInvalidState
	} else {
		generation++
	}
	if err != nil {
		return domain.Reservation{}, fmt.Errorf("create reservation ID: %w", err)
	}

	if err := lockTenantCounter(ctx, tx, tenantHash, int(cmd.SlotCost()), !rerank); err != nil {
		return domain.Reservation{}, err
	}
	if rerank {
		var grantState string
		var grantCost int
		var grantTenant []byte
		if err := tx.QueryRow(ctx, `SELECT state, slot_cost, tenant_hash
			FROM admission_grants WHERE reservation_id=$1 FOR UPDATE`, reservationID).
			Scan(&grantState, &grantCost, &grantTenant); err != nil {
			return domain.Reservation{}, fmt.Errorf("lock retained admission grant: %w", err)
		}
		if grantState != "retained_rerank" || grantCost != int(cmd.SlotCost()) || !equalBytes(grantTenant, tenantHash[:]) {
			return domain.Reservation{}, domain.ErrInvalidState
		}
	}

	tag, err := tx.Exec(ctx, `
		UPDATE scheduler_backends
		SET reserved_slots = reserved_slots + $7
		WHERE cluster = $1 AND namespace = $2 AND logical_engine = $3 AND pod_uid = $4
		  AND endpoint_epoch = $5 AND recovery_epoch = $6 AND model = $8
		  AND healthy AND NOT drain_active AND eligible_until > clock_timestamp()
		  AND admission_limit - reserved_slots - orphaned_slots >= $7`,
		identity.Cluster(), identity.Namespace(), identity.LogicalEngine(), identity.PodUID(), identity.EndpointEpoch(), identity.RecoveryEpoch(), cmd.SlotCost(), cmd.Model())
	if err != nil {
		return domain.Reservation{}, fmt.Errorf("reserve backend capacity: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.Reservation{}, domain.ErrNoCapacity
	}

	capability := make([]byte, 32)
	if _, err := rand.Read(capability); err != nil {
		return domain.Reservation{}, fmt.Errorf("create reservation capability: %w", err)
	}
	ciphertext, err := s.encrypt(capability, reservationID)
	if err != nil {
		return domain.Reservation{}, err
	}
	capabilityHash := domain.CapabilityHash(capability)
	if rerank {
		tag, err := tx.Exec(ctx, `
			UPDATE scheduler_reservations
			SET request_generation=$2, cluster=$3, namespace=$4, logical_engine=$5, pod_uid=$6,
			    endpoint_epoch=$7, recovery_epoch=$8, state='reserved',
			    capability_ciphertext=$9, capability_hash=$10, updated_at=clock_timestamp()
			WHERE reservation_id=$1 AND state='abandoned_rerank'
			  AND execution_deadline > clock_timestamp()`,
			reservationID, generation, identity.Cluster(), identity.Namespace(), identity.LogicalEngine(), identity.PodUID(),
			identity.EndpointEpoch(), identity.RecoveryEpoch(), ciphertext, capabilityHash[:])
		if err != nil {
			return domain.Reservation{}, fmt.Errorf("persist rerank reservation: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return domain.Reservation{}, domain.ErrNoCapacity
		}
		if _, err := tx.Exec(ctx, `UPDATE admission_grants SET state='active_reserved', updated_at=clock_timestamp()
			WHERE reservation_id=$1 AND state='retained_rerank'`, reservationID); err != nil {
			return domain.Reservation{}, fmt.Errorf("reactivate retained admission grant: %w", err)
		}
	} else {
		var lookupVersion, digestVersion *uint32
		var lookupHMAC, requestDigest []byte
		if cmd.HasIdempotencyKey() {
			lookupValue, digestValue := lookupWrite.Value(), digestWrite.Value()
			lookupVersion, digestVersion = new(lookupWrite.Version()), new(digestWrite.Version())
			lookupHMAC, requestDigest = lookupValue[:], digestValue[:]
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO scheduler_reservations (
				reservation_id, request_id, attempt_id, command_hash, tenant_hash,
				lookup_version, lookup_hmac, digest_version, request_digest,
				model, slot_cost, request_generation, cluster, namespace, logical_engine, pod_uid,
				endpoint_epoch, recovery_epoch, state, capability_ciphertext, capability_hash, execution_deadline)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,1,$12,$13,$14,$15,$16,$17,'reserved',$18,$19,clock_timestamp()+($20 * interval '1 millisecond'))`,
			reservationID, cmd.RequestID(), cmd.AttemptID(), commandHash[:], tenantHash[:],
			lookupVersion, lookupHMAC, digestVersion, requestDigest,
			cmd.Model(), cmd.SlotCost(), identity.Cluster(), identity.Namespace(), identity.LogicalEngine(), identity.PodUID(),
			identity.EndpointEpoch(), identity.RecoveryEpoch(), ciphertext, capabilityHash[:], cmd.ExecutionBudget().Milliseconds())
		if err != nil {
			return domain.Reservation{}, fmt.Errorf("persist reservation: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO admission_grants (
			grant_id, request_id, reservation_id, tenant_hash, slot_cost, state, execution_deadline, classification_after)
			SELECT $1, request_id, reservation_id, tenant_hash, slot_cost, 'active_reserved',
			       execution_deadline, execution_deadline
			FROM scheduler_reservations WHERE reservation_id=$2`, "grant_"+reservationID, reservationID); err != nil {
			return domain.Reservation{}, fmt.Errorf("persist admission grant: %w", err)
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

func (s *SchedulerStore) existingReservation(ctx context.Context, tx pgx.Tx, cmd domain.ScheduleCommand, commandHash, tenantHash [32]byte) (domain.Reservation, bool, error) {
	var id, attempt, state string
	var generation uint64
	var storedHash, ciphertext, storedDigest []byte
	var storedDigestVersion *uint32
	var err error
	if cmd.HasIdempotencyKey() {
		matches := 0
		for _, candidate := range cmd.IdempotencyCandidates() {
			value := candidate.Value()
			rowErr := tx.QueryRow(ctx, `SELECT reservation_id, attempt_id, state, request_generation, command_hash,
				capability_ciphertext, digest_version, request_digest
				FROM scheduler_reservations
				WHERE tenant_hash=$1 AND lookup_version=$2 AND lookup_hmac=$3 FOR UPDATE`,
				tenantHash[:], candidate.Version(), value[:]).
				Scan(&id, &attempt, &state, &generation, &storedHash, &ciphertext, &storedDigestVersion, &storedDigest)
			if errors.Is(rowErr, pgx.ErrNoRows) {
				continue
			}
			if rowErr != nil {
				return domain.Reservation{}, false, fmt.Errorf("lookup idempotent reservation: %w", rowErr)
			}
			matches++
		}
		if matches == 0 {
			return domain.Reservation{}, false, nil
		}
		if matches != 1 || storedDigestVersion == nil {
			return domain.Reservation{}, false, domain.ErrInvalidState
		}
		digest, ok := digestCandidate(cmd, *storedDigestVersion)
		if !ok {
			return domain.Reservation{}, false, domain.ErrInvalidState
		}
		digestValue := digest.Value()
		if !equalBytes(storedDigest, digestValue[:]) {
			return domain.Reservation{}, false, domain.ErrIdempotencyConflict
		}
		if attempt != cmd.AttemptID() {
			switch state {
			case "reserved", "dispatch_authorized":
				return domain.Reservation{}, false, domain.ErrRequestInProgress
			case "orphaned":
				var debtState string
				err := tx.QueryRow(ctx, `SELECT state FROM capacity_debts WHERE reservation_id=$1`, id).Scan(&debtState)
				if err != nil {
					return domain.Reservation{}, false, domain.ErrInvalidState
				}
				if debtState == "active" {
					return domain.Reservation{}, false, domain.ErrRequestInProgress
				}
				if debtState == "resolved_identity_gone" {
					return domain.Reservation{}, false, domain.ErrRequestNotReplayable
				}
				return domain.Reservation{}, false, domain.ErrInvalidState
			default:
				return domain.Reservation{}, false, domain.ErrRequestNotReplayable
			}
		}
		if !equalBytes(storedHash, commandHash[:]) {
			return domain.Reservation{}, false, domain.ErrInvalidState
		}
	} else {
		err = tx.QueryRow(ctx, `SELECT reservation_id, attempt_id, state, request_generation, command_hash, capability_ciphertext
			FROM scheduler_reservations WHERE request_id=$1 FOR UPDATE`, cmd.RequestID()).
			Scan(&id, &attempt, &state, &generation, &storedHash, &ciphertext)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Reservation{}, false, nil
		}
		if err != nil {
			return domain.Reservation{}, false, fmt.Errorf("recover reservation: %w", err)
		}
		if attempt != cmd.AttemptID() || !equalBytes(storedHash, commandHash[:]) {
			return domain.Reservation{}, false, domain.ErrInvalidState
		}
	}
	if state == "abandoned_rerank" {
		return domain.Reservation{}, false, nil
	}

	capability, err := s.decrypt(ciphertext, id)
	if err != nil {
		return domain.Reservation{}, false, err
	}
	ref, err := domain.NewReservationRef(id, generation, capability)
	if err != nil {
		return domain.Reservation{}, false, err
	}
	return domain.NewReservation(ref), true, nil
}

func lockAndValidateCryptoVersions(ctx context.Context, tx pgx.Tx, cmd domain.ScheduleCommand, tenantHash [32]byte) error {
	var lookupWriteVersion, digestWriteVersion uint32
	if err := tx.QueryRow(ctx, `SELECT lookup_write_version, digest_write_version
		FROM scheduler_crypto_versions WHERE singleton FOR SHARE`).
		Scan(&lookupWriteVersion, &digestWriteVersion); err != nil {
		return fmt.Errorf("read coordinated crypto versions: %w", err)
	}
	if lookupWriteVersion != cmd.LookupWriteVersion() || digestWriteVersion != cmd.DigestWriteVersion() {
		return domain.ErrInvalidState
	}
	for _, candidate := range cmd.IdempotencyCandidates() {
		value := candidate.Value()
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended(encode($1::bytea || $2::bytea, 'hex'), 0))`, tenantHash[:], value[:]); err != nil {
			return fmt.Errorf("lock idempotency key: %w", err)
		}
	}
	return nil
}

func lookupWriteCandidate(cmd domain.ScheduleCommand) (domain.IdempotencyLookupCandidate, error) {
	for _, candidate := range cmd.IdempotencyCandidates() {
		if candidate.Version() == cmd.LookupWriteVersion() {
			return candidate, nil
		}
	}
	return domain.IdempotencyLookupCandidate{}, domain.ErrInvalidState
}

func digestWriteCandidate(cmd domain.ScheduleCommand) (domain.RequestDigestCandidate, error) {
	candidate, ok := digestCandidate(cmd, cmd.DigestWriteVersion())
	if !ok {
		return domain.RequestDigestCandidate{}, domain.ErrInvalidState
	}
	return candidate, nil
}

func digestCandidate(cmd domain.ScheduleCommand, version uint32) (domain.RequestDigestCandidate, bool) {
	for _, candidate := range cmd.DigestCandidates() {
		if candidate.Version() == version {
			return candidate, true
		}
	}
	return domain.RequestDigestCandidate{}, false
}

// The mutation methods use Read Committed deliberately. The reservation row serializes state
// transitions, followed by FOR UPDATE locks on every changed tenant/grant and exact backend row.
// Conditional counter updates then make duplicate calls exact-once without Serializable retries.

func (s *SchedulerStore) PrepareDispatch(ctx context.Context, ref domain.ReservationRef) (domain.DispatchTarget, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.DispatchTarget{}, fmt.Errorf("begin dispatch authorization: %w", err)
	}
	defer tx.Rollback(ctx)

	row, err := lockReservation(ctx, tx, ref)
	if err != nil {
		return domain.DispatchTarget{}, err
	}
	if (row.state != "reserved" && row.state != "dispatch_authorized") || !row.beforeDeadline {
		return domain.DispatchTarget{}, domain.ErrInvalidState
	}
	var endpoint string
	var healthy, draining bool
	var eligible bool
	err = tx.QueryRow(ctx, `SELECT endpoint, healthy, drain_active, eligible_until > clock_timestamp()
		FROM scheduler_backends WHERE cluster=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4
		AND endpoint_epoch=$5 AND recovery_epoch=$6 FOR UPDATE`, row.cluster, row.namespace, row.engine, row.podUID, row.endpointEpoch, row.recoveryEpoch).
		Scan(&endpoint, &healthy, &draining, &eligible)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.DispatchTarget{}, domain.ErrStaleTarget
	}
	if err != nil {
		return domain.DispatchTarget{}, fmt.Errorf("recheck dispatch target: %w", err)
	}
	if !healthy || draining || !eligible {
		return domain.DispatchTarget{}, domain.ErrStaleTarget
	}
	if row.state == "reserved" {
		if _, err := tx.Exec(ctx, `UPDATE scheduler_reservations SET state='dispatch_authorized', updated_at=clock_timestamp() WHERE reservation_id=$1`, ref.ID()); err != nil {
			return domain.DispatchTarget{}, fmt.Errorf("authorize dispatch: %w", err)
		}
	}
	identity, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: row.cluster, Namespace: row.namespace, LogicalEngine: row.engine, PodUID: row.podUID, EndpointEpoch: row.endpointEpoch, RecoveryEpoch: row.recoveryEpoch})
	if err != nil {
		return domain.DispatchTarget{}, err
	}
	target, err := domain.NewDispatchTarget(endpoint, identity)
	if err != nil {
		return domain.DispatchTarget{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.DispatchTarget{}, fmt.Errorf("commit dispatch authorization: %w", err)
	}
	return target, nil
}

func (s *SchedulerStore) AbandonBeforeDispatch(ctx context.Context, ref domain.ReservationRef, reason domain.RerankReason) error {
	if reason != domain.RerankStaleTarget {
		return domain.ErrInvalidState
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin rerank abandonment: %w", err)
	}
	defer tx.Rollback(ctx)
	row, err := lockReservation(ctx, tx, ref)
	if err != nil {
		return err
	}
	if row.state == "abandoned_rerank" {
		return tx.Commit(ctx)
	}
	if row.state != "reserved" {
		return domain.ErrInvalidState
	}
	if err := transitionGrant(ctx, tx, row, "active_reserved", "retained_rerank", grantCounterUnchanged); err != nil {
		return err
	}
	if err := updateBackendCapacity(ctx, tx, row, 0); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE scheduler_reservations
		SET state='abandoned_rerank', updated_at=clock_timestamp()
		WHERE reservation_id=$1`, ref.ID()); err != nil {
		return fmt.Errorf("persist rerank abandonment: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rerank abandonment: %w", err)
	}
	return nil
}

func (s *SchedulerStore) GiveUpBeforeDispatch(ctx context.Context, ref domain.ReservationRef, reason domain.GiveUpReason) error {
	if reason < domain.GiveUpCanceled || reason > domain.GiveUpReranksExhausted {
		return domain.ErrInvalidState
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin pre-dispatch give-up: %w", err)
	}
	defer tx.Rollback(ctx)
	row, err := lockReservation(ctx, tx, ref)
	if err != nil {
		return err
	}
	if row.state == "given_up" {
		return tx.Commit(ctx)
	}
	if row.state != "reserved" && row.state != "abandoned_rerank" {
		return domain.ErrInvalidState
	}
	grantState := "active_reserved"
	if row.state == "abandoned_rerank" {
		grantState = "retained_rerank"
	}
	if err := transitionGrant(ctx, tx, row, grantState, "released", grantCounterRelease); err != nil {
		return err
	}
	if row.state == "reserved" {
		if err := updateBackendCapacity(ctx, tx, row, 0); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE scheduler_reservations
		SET state='given_up', updated_at=clock_timestamp()
		WHERE reservation_id=$1`, ref.ID()); err != nil {
		return fmt.Errorf("persist pre-dispatch give-up: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit pre-dispatch give-up: %w", err)
	}
	return nil
}

func (s *SchedulerStore) Finalize(ctx context.Context, ref domain.ReservationRef, proof domain.TerminalProof) error {
	if proof != domain.TerminalProofProviderFinish && proof != domain.TerminalProofNotSent {
		return domain.ErrInvalidState
	}
	return s.releaseTerminal(ctx, ref)
}

func (s *SchedulerStore) MarkAmbiguous(ctx context.Context, ref domain.ReservationRef, cause domain.AmbiguousCause) error {
	var debtCause string
	switch cause {
	case domain.AmbiguousTransport:
		debtCause = "ambiguous_transport"
	case domain.AmbiguousCanceled:
		debtCause = "ambiguous_canceled"
	case domain.AmbiguousProtocol:
		debtCause = "ambiguous_protocol"
	default:
		return domain.ErrInvalidState
	}
	return s.orphan(ctx, ref, debtCause)
}

func (s *SchedulerStore) ClassifyExpired(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 1000 {
		return 0, errors.New("invalid classification limit")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin reservation classification: %w", err)
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT reservation_id, state, cluster, namespace, logical_engine, pod_uid,
		       endpoint_epoch, recovery_epoch, slot_cost, tenant_hash
		FROM scheduler_reservations
		WHERE state IN ('reserved','abandoned_rerank','dispatch_authorized') AND execution_deadline <= clock_timestamp()
		ORDER BY execution_deadline, reservation_id
		FOR UPDATE SKIP LOCKED LIMIT $1`, limit)
	if err != nil {
		return 0, fmt.Errorf("select expired reservations: %w", err)
	}
	type expiredReservation struct {
		id  string
		row reservationRow
	}
	expired := make([]expiredReservation, 0, limit)
	for rows.Next() {
		var item expiredReservation
		if err := rows.Scan(&item.id, &item.row.state, &item.row.cluster, &item.row.namespace, &item.row.engine,
			&item.row.podUID, &item.row.endpointEpoch, &item.row.recoveryEpoch, &item.row.slotCost, &item.row.tenantHash); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan expired reservation: %w", err)
		}
		item.row.reservationID = item.id
		expired = append(expired, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("select expired reservations: %w", err)
	}
	rows.Close()
	for _, item := range expired {
		terminal := "given_up"
		grantFrom := "active_reserved"
		counterEffect := grantCounterRelease
		orphanDelta := 0
		if item.row.state == "dispatch_authorized" {
			terminal = "orphaned"
			counterEffect = grantCounterOrphan
			orphanDelta = item.row.slotCost
		} else if item.row.state == "abandoned_rerank" {
			grantFrom = "retained_rerank"
		}
		grantTo := "released"
		if terminal == "orphaned" {
			grantTo = "orphaned"
		}
		if err := transitionGrant(ctx, tx, item.row, grantFrom, grantTo, counterEffect); err != nil {
			return 0, err
		}
		if item.row.state != "abandoned_rerank" {
			if err := updateBackendCapacity(ctx, tx, item.row, orphanDelta); err != nil {
				return 0, err
			}
		}
		tag, err := tx.Exec(ctx, `UPDATE scheduler_reservations
			SET state=$2, updated_at=clock_timestamp()
			WHERE reservation_id=$1 AND state=$3`, item.id, terminal, item.row.state)
		if err != nil {
			return 0, fmt.Errorf("classify expired reservation: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return 0, domain.ErrInvalidState
		}
		if terminal == "orphaned" {
			if err := insertCapacityDebt(ctx, tx, item.row, "classification_timeout"); err != nil {
				return 0, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit reservation classification: %w", err)
	}
	return len(expired), nil
}

func (s *SchedulerStore) releaseTerminal(ctx context.Context, ref domain.ReservationRef) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin terminal reservation release: %w", err)
	}
	defer tx.Rollback(ctx)
	row, err := lockReservation(ctx, tx, ref)
	if err != nil {
		return err
	}
	if row.state == "released" {
		return tx.Commit(ctx)
	}
	if row.state != "dispatch_authorized" {
		return domain.ErrInvalidState
	}
	if err := transitionGrant(ctx, tx, row, "active_reserved", "released", grantCounterRelease); err != nil {
		return err
	}
	if err := updateBackendCapacity(ctx, tx, row, 0); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE scheduler_reservations
		SET state='released', updated_at=clock_timestamp()
		WHERE reservation_id=$1 AND state='dispatch_authorized'`, ref.ID())
	if err != nil {
		return fmt.Errorf("complete terminal reservation release: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit terminal reservation release: %w", err)
	}
	return nil
}

func (s *SchedulerStore) orphan(ctx context.Context, ref domain.ReservationRef, cause string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin orphan reservation transition: %w", err)
	}
	defer tx.Rollback(ctx)
	row, err := lockReservation(ctx, tx, ref)
	if err != nil {
		return err
	}
	if row.state == "orphaned" {
		if err := insertCapacityDebt(ctx, tx, row, cause); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if row.state != "dispatch_authorized" {
		return domain.ErrInvalidState
	}
	if err := transitionGrant(ctx, tx, row, "active_reserved", "orphaned", grantCounterOrphan); err != nil {
		return err
	}
	if err := updateBackendCapacity(ctx, tx, row, row.slotCost); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE scheduler_reservations
		SET state='orphaned', updated_at=clock_timestamp()
		WHERE reservation_id=$1 AND state='dispatch_authorized'`, ref.ID())
	if err != nil {
		return fmt.Errorf("persist orphaned reservation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	if err := insertCapacityDebt(ctx, tx, row, cause); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit orphan reservation transition: %w", err)
	}
	return nil
}

func insertCapacityDebt(ctx context.Context, tx pgx.Tx, row reservationRow, cause string) error {
	debtID := "debt_" + row.reservationID
	tag, err := tx.Exec(ctx, `INSERT INTO capacity_debts (
		debt_id, reservation_id, tenant_hash, cluster, namespace, logical_engine, pod_uid,
		endpoint_epoch, recovery_epoch, slot_cost, cause, state)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'active')
		ON CONFLICT DO NOTHING`,
		debtID, row.reservationID, row.tenantHash, row.cluster, row.namespace, row.engine,
		row.podUID, row.endpointEpoch, row.recoveryEpoch, row.slotCost, cause)
	if err != nil {
		return fmt.Errorf("insert capacity debt: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	var (
		reservationID, cluster, namespace, engine, podUID, storedCause, state string
		tenantHash                                                            []byte
		endpointEpoch, recoveryEpoch                                          uint64
		slotCost                                                              int
	)
	err = tx.QueryRow(ctx, `SELECT reservation_id, tenant_hash, cluster, namespace, logical_engine,
		pod_uid, endpoint_epoch, recovery_epoch, slot_cost, cause, state
		FROM capacity_debts WHERE debt_id=$1 FOR UPDATE`, debtID).
		Scan(&reservationID, &tenantHash, &cluster, &namespace, &engine, &podUID,
			&endpointEpoch, &recoveryEpoch, &slotCost, &storedCause, &state)
	if err != nil {
		return domain.ErrInvalidState
	}
	if reservationID != row.reservationID || !equalBytes(tenantHash, row.tenantHash) ||
		cluster != row.cluster || namespace != row.namespace || engine != row.engine ||
		podUID != row.podUID || endpointEpoch != row.endpointEpoch ||
		recoveryEpoch != row.recoveryEpoch || slotCost != row.slotCost ||
		storedCause != cause || (state != "active" && state != "resolved_identity_gone") {
		return domain.ErrInvalidState
	}
	return nil
}

type grantCounterEffect uint8

const (
	grantCounterUnchanged grantCounterEffect = iota
	grantCounterRelease
	grantCounterOrphan
	grantCounterResolveOrphan
)

func lockTenantCounter(ctx context.Context, tx pgx.Tx, tenantHash [32]byte, slotCost int, increment bool) error {
	var grantLimit, activeGrants, orphanedGrants int
	err := tx.QueryRow(ctx, `SELECT grant_limit, active_grants, orphaned_grants
		FROM tenant_counters WHERE tenant_hash=$1 FOR UPDATE`, tenantHash[:]).
		Scan(&grantLimit, &activeGrants, &orphanedGrants)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNoCapacity
	}
	if err != nil {
		return fmt.Errorf("lock tenant counter: %w", err)
	}
	if !increment {
		return nil
	}
	if slotCost > grantLimit-activeGrants-orphanedGrants {
		return domain.ErrNoCapacity
	}
	if _, err := tx.Exec(ctx, `UPDATE tenant_counters
		SET active_grants=active_grants+$2, version=version+1 WHERE tenant_hash=$1`,
		tenantHash[:], slotCost); err != nil {
		return fmt.Errorf("reserve tenant admission grant: %w", err)
	}
	return nil
}

func transitionGrant(ctx context.Context, tx pgx.Tx, row reservationRow, from, to string, effect grantCounterEffect) error {
	var contribution int
	switch effect {
	case grantCounterUnchanged:
	case grantCounterRelease, grantCounterOrphan:
		if err := tx.QueryRow(ctx, `SELECT active_grants
			FROM tenant_counters WHERE tenant_hash=$1 FOR UPDATE`, row.tenantHash).
			Scan(&contribution); err != nil {
			return fmt.Errorf("lock active tenant contribution: %w", err)
		}
	case grantCounterResolveOrphan:
		if err := tx.QueryRow(ctx, `SELECT orphaned_grants
			FROM tenant_counters WHERE tenant_hash=$1 FOR UPDATE`, row.tenantHash).
			Scan(&contribution); err != nil {
			return fmt.Errorf("lock orphaned tenant contribution: %w", err)
		}
	default:
		return domain.ErrInvalidState
	}
	var state string
	var slotCost int
	var tenantHash []byte
	if err := tx.QueryRow(ctx, `SELECT state, slot_cost, tenant_hash
		FROM admission_grants WHERE reservation_id=$1 FOR UPDATE`, row.reservationID).
		Scan(&state, &slotCost, &tenantHash); err != nil {
		return fmt.Errorf("lock admission grant: %w", err)
	}
	if state != from || slotCost != row.slotCost || !equalBytes(tenantHash, row.tenantHash) {
		return domain.ErrInvalidState
	}

	var tag pgconn.CommandTag
	var err error
	switch effect {
	case grantCounterUnchanged:
	case grantCounterRelease:
		if contribution < slotCost {
			return domain.ErrInvalidState
		}
		tag, err = tx.Exec(ctx, `UPDATE tenant_counters
			SET active_grants=active_grants-$2, version=version+1
			WHERE tenant_hash=$1 AND active_grants >= $2`, row.tenantHash, slotCost)
	case grantCounterOrphan:
		if contribution < slotCost {
			return domain.ErrInvalidState
		}
		tag, err = tx.Exec(ctx, `UPDATE tenant_counters
			SET active_grants=active_grants-$2, orphaned_grants=orphaned_grants+$2, version=version+1
			WHERE tenant_hash=$1 AND active_grants >= $2`, row.tenantHash, slotCost)
	case grantCounterResolveOrphan:
		if contribution < slotCost {
			return domain.ErrInvalidState
		}
		tag, err = tx.Exec(ctx, `UPDATE tenant_counters
			SET orphaned_grants=orphaned_grants-$2, version=version+1
			WHERE tenant_hash=$1 AND orphaned_grants >= $2`, row.tenantHash, slotCost)
	default:
		return domain.ErrInvalidState
	}
	if err != nil {
		return fmt.Errorf("update tenant contribution: %w", err)
	}
	if effect != grantCounterUnchanged && tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}

	tag, err = tx.Exec(ctx, `UPDATE admission_grants SET state=$2, updated_at=clock_timestamp()
		WHERE reservation_id=$1 AND state=$3`, row.reservationID, to, from)
	if err != nil {
		return fmt.Errorf("transition admission grant: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	return nil
}

func updateBackendCapacity(ctx context.Context, tx pgx.Tx, row reservationRow, orphanDelta int) error {
	tag, err := tx.Exec(ctx, `UPDATE scheduler_backends
		SET reserved_slots=reserved_slots-$7, orphaned_slots=orphaned_slots+$8
		WHERE cluster=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4
		  AND endpoint_epoch=$5 AND recovery_epoch=$6 AND reserved_slots >= $7`,
		row.cluster, row.namespace, row.engine, row.podUID, row.endpointEpoch, row.recoveryEpoch, row.slotCost, orphanDelta)
	if err != nil {
		return fmt.Errorf("update backend capacity accounting: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	return nil
}

type reservationRow struct {
	state, cluster, namespace, engine, podUID string
	reservationID                             string
	tenantHash                                []byte
	endpointEpoch, recoveryEpoch              uint64
	slotCost                                  int
	beforeDeadline                            bool
}

func lockReservation(ctx context.Context, tx pgx.Tx, ref domain.ReservationRef) (reservationRow, error) {
	var row reservationRow
	var generation uint64
	var capabilityHash []byte
	err := tx.QueryRow(ctx, `SELECT state, request_generation, capability_hash, cluster, namespace, logical_engine,
		       pod_uid, endpoint_epoch, recovery_epoch, slot_cost, execution_deadline > clock_timestamp(), tenant_hash
		FROM scheduler_reservations WHERE reservation_id=$1 FOR UPDATE`, ref.ID()).
		Scan(&row.state, &generation, &capabilityHash, &row.cluster, &row.namespace, &row.engine, &row.podUID,
			&row.endpointEpoch, &row.recoveryEpoch, &row.slotCost, &row.beforeDeadline, &row.tenantHash)
	row.reservationID = ref.ID()
	if errors.Is(err, pgx.ErrNoRows) {
		return reservationRow{}, domain.ErrInvalidReference
	}
	if err != nil {
		return reservationRow{}, fmt.Errorf("lock reservation: %w", err)
	}
	if generation != ref.Generation() || !domain.CapabilityMatches(ref.Capability(), capabilityHash) {
		return reservationRow{}, domain.ErrInvalidReference
	}
	return row, nil
}

func (s *SchedulerStore) encrypt(plaintext []byte, reservationID string) ([]byte, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("create capability nonce: %w", err)
	}
	return s.aead.Seal(nonce, nonce, plaintext, []byte(reservationID)), nil
}

func (s *SchedulerStore) decrypt(ciphertext []byte, reservationID string) ([]byte, error) {
	if len(ciphertext) < s.aead.NonceSize() {
		return nil, errors.New("invalid encrypted capability")
	}
	nonce := ciphertext[:s.aead.NonceSize()]
	plaintext, err := s.aead.Open(nil, nonce, ciphertext[s.aead.NonceSize():], []byte(reservationID))
	if err != nil {
		return nil, fmt.Errorf("decrypt reservation capability: %w", err)
	}
	return plaintext, nil
}

func hashCommand(cmd domain.ScheduleCommand) [32]byte {
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "%s\x00%s\x00%s\x00%s\x00%d\x00%d\x00%d\x00%d",
		cmd.RequestID(), cmd.AttemptID(), cmd.Tenant(), cmd.Model(), cmd.SlotCost(), cmd.ExecutionBudget(),
		cmd.LookupWriteVersion(), cmd.DigestWriteVersion())
	for _, candidate := range cmd.IdempotencyCandidates() {
		value := candidate.Value()
		_, _ = fmt.Fprintf(hash, "\x00%d\x00%x", candidate.Version(), value)
	}
	for _, candidate := range cmd.DigestCandidates() {
		value := candidate.Value()
		_, _ = fmt.Fprintf(hash, "\x00%d\x00%x", candidate.Version(), value)
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func randomID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(raw[:]), nil
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var different byte
	for i := range left {
		different |= left[i] ^ right[i]
	}
	return different == 0
}
