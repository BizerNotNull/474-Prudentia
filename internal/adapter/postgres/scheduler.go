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

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/BizerNotNull/474-Prudentia/internal/scheduling"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SchedulerStore struct {
	pool *pgxpool.Pool
	aead cipher.AEAD
}

func NewSchedulerStore(pool *pgxpool.Pool, capabilityKey []byte) (*SchedulerStore, error) {
	if pool == nil || len(capabilityKey) != 32 {
		return nil, errors.New("invalid scheduler store configuration")
	}
	block, err := aes.NewCipher(capabilityKey)
	if err != nil {
		return nil, fmt.Errorf("create capability cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create capability AEAD: %w", err)
	}
	return &SchedulerStore{pool: pool, aead: aead}, nil
}

func (s *SchedulerStore) Candidates(ctx context.Context, cmd domain.ScheduleCommand) ([]scheduling.Candidate, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch,
		       admission_limit - reserved_slots - orphaned_slots
		FROM scheduler_backends
		WHERE model = $1 AND healthy AND NOT drain_active AND eligible_until > clock_timestamp()
		  AND admission_limit - reserved_slots - orphaned_slots >= $2`, cmd.Model(), cmd.SlotCost())
	if err != nil {
		return nil, fmt.Errorf("read scheduling candidates: %w", err)
	}
	defer rows.Close()

	var candidates []scheduling.Candidate
	for rows.Next() {
		var p domain.WorkloadIdentityParams
		var available int32
		if err := rows.Scan(&p.Cluster, &p.Namespace, &p.LogicalEngine, &p.PodUID, &p.EndpointEpoch, &p.RecoveryEpoch, &available); err != nil {
			return nil, fmt.Errorf("scan scheduling candidate: %w", err)
		}
		identity, err := domain.NewWorkloadIdentity(p)
		if err != nil {
			return nil, fmt.Errorf("decode scheduling candidate: %w", err)
		}
		candidates = append(candidates, scheduling.Candidate{Identity: identity, AvailableSlots: uint32(available)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read scheduling candidates: %w", err)
	}
	return candidates, nil
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
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
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

	reservationID, err := randomID("res_")
	if err != nil {
		return domain.Reservation{}, fmt.Errorf("create reservation ID: %w", err)
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
	if err := tx.Commit(ctx); err != nil {
		return domain.Reservation{}, fmt.Errorf("commit reservation: %w", err)
	}
	ref, err := domain.NewReservationRef(reservationID, 1, capability)
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
			case "reserved", "dispatch_authorized", "orphaned":
				return domain.Reservation{}, false, domain.ErrRequestInProgress
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

func (s *SchedulerStore) PrepareDispatch(ctx context.Context, ref domain.ReservationRef) (domain.DispatchTarget, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
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

func (s *SchedulerStore) GiveUpBeforeDispatch(ctx context.Context, ref domain.ReservationRef, reason domain.GiveUpReason) error {
	if reason < domain.GiveUpCanceled || reason > domain.GiveUpReranksExhausted {
		return domain.ErrInvalidState
	}
	return s.release(ctx, ref, "given_up", false, "reserved")
}

func (s *SchedulerStore) Finalize(ctx context.Context, ref domain.ReservationRef, proof domain.TerminalProof) error {
	if proof != domain.TerminalProofProviderFinish && proof != domain.TerminalProofNotSent {
		return domain.ErrInvalidState
	}
	return s.release(ctx, ref, "released", false, "dispatch_authorized")
}

func (s *SchedulerStore) MarkAmbiguous(ctx context.Context, ref domain.ReservationRef, cause domain.AmbiguousCause) error {
	if cause < domain.AmbiguousTransport || cause > domain.AmbiguousProtocol {
		return domain.ErrInvalidState
	}
	return s.release(ctx, ref, "orphaned", true, "dispatch_authorized")
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
	rows, err := tx.Query(ctx, `SELECT reservation_id, state, cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch, slot_cost
		FROM scheduler_reservations
		WHERE state IN ('reserved','dispatch_authorized') AND execution_deadline <= clock_timestamp()
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
		if err := rows.Scan(&item.id, &item.row.state, &item.row.cluster, &item.row.namespace, &item.row.engine, &item.row.podUID, &item.row.endpointEpoch, &item.row.recoveryEpoch, &item.row.slotCost); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan expired reservation: %w", err)
		}
		expired = append(expired, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("select expired reservations: %w", err)
	}
	rows.Close()
	for _, item := range expired {
		terminal := "given_up"
		orphanDelta := 0
		if item.row.state == "dispatch_authorized" {
			terminal = "orphaned"
			orphanDelta = item.row.slotCost
		}
		tag, err := tx.Exec(ctx, `UPDATE scheduler_backends
			SET reserved_slots=reserved_slots-$7, orphaned_slots=orphaned_slots+$8
			WHERE cluster=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4
			AND endpoint_epoch=$5 AND recovery_epoch=$6 AND reserved_slots >= $7`,
			item.row.cluster, item.row.namespace, item.row.engine, item.row.podUID, item.row.endpointEpoch, item.row.recoveryEpoch, item.row.slotCost, orphanDelta)
		if err != nil {
			return 0, fmt.Errorf("classify expired capacity: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return 0, domain.ErrInvalidState
		}
		if _, err := tx.Exec(ctx, `UPDATE scheduler_reservations SET state=$2, updated_at=clock_timestamp() WHERE reservation_id=$1`, item.id, terminal); err != nil {
			return 0, fmt.Errorf("classify expired reservation: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit reservation classification: %w", err)
	}
	return len(expired), nil
}

func (s *SchedulerStore) release(ctx context.Context, ref domain.ReservationRef, terminal string, debt bool, allowed string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("begin reservation transition: %w", err)
	}
	defer tx.Rollback(ctx)
	row, err := lockReservation(ctx, tx, ref)
	if err != nil {
		return err
	}
	if row.state == terminal {
		return tx.Commit(ctx)
	}
	if row.state != allowed {
		return domain.ErrInvalidState
	}
	orphanDelta := 0
	if debt {
		orphanDelta = row.slotCost
	}
	tag, err := tx.Exec(ctx, `UPDATE scheduler_backends
		SET reserved_slots=reserved_slots-$7, orphaned_slots=orphaned_slots+$8
		WHERE cluster=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4 AND endpoint_epoch=$5 AND recovery_epoch=$6 AND reserved_slots >= $7`,
		row.cluster, row.namespace, row.engine, row.podUID, row.endpointEpoch, row.recoveryEpoch, row.slotCost, orphanDelta)
	if err != nil {
		return fmt.Errorf("update capacity accounting: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrInvalidState
	}
	if _, err := tx.Exec(ctx, `UPDATE scheduler_reservations SET state=$2, updated_at=clock_timestamp() WHERE reservation_id=$1`, ref.ID(), terminal); err != nil {
		return fmt.Errorf("complete reservation transition: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit reservation transition: %w", err)
	}
	return nil
}

type reservationRow struct {
	state, cluster, namespace, engine, podUID string
	endpointEpoch, recoveryEpoch              uint64
	slotCost                                  int
	beforeDeadline                            bool
}

func lockReservation(ctx context.Context, tx pgx.Tx, ref domain.ReservationRef) (reservationRow, error) {
	var row reservationRow
	var generation uint64
	var capabilityHash []byte
	err := tx.QueryRow(ctx, `SELECT state, request_generation, capability_hash, cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch, slot_cost, execution_deadline > clock_timestamp()
		FROM scheduler_reservations WHERE reservation_id=$1 FOR UPDATE`, ref.ID()).Scan(&row.state, &generation, &capabilityHash, &row.cluster, &row.namespace, &row.engine, &row.podUID, &row.endpointEpoch, &row.recoveryEpoch, &row.slotCost, &row.beforeDeadline)
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
