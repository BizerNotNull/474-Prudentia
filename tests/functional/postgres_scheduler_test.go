package functional_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	postgresadapter "github.com/BizerNotNull/474-Prudentia/internal/adapter/postgres"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const testCapabilityKey = "0123456789abcdef0123456789abcdef"

type reservationLedgerRow struct {
	state             string
	requestGeneration uint64
	capabilityHash    []byte
	executionDeadline time.Time
	podUID            string
}

type backendCapacity struct {
	reserved int
	orphaned int
}

func TestAuthoritativeLedgerCutover(t *testing.T) {
	ctx := context.Background()
	root := repositoryRoot(t)
	migrations, err := filepath.Glob(filepath.Join(root, "migrations", "*.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(migrations)
	pool := startPostgres(t, migrations)
	store, err := postgresadapter.NewSchedulerStore(pool, []byte(testCapabilityKey))
	if err != nil {
		t.Fatal(err)
	}
	actor := sha256.Sum256([]byte("functional-ledger"))
	if _, err := pool.Exec(ctx, `INSERT INTO system_admission_state(
		cluster_id,recovery_epoch,admission_state,dispatch_state,schema_write_version,
		lookup_write_version,digest_write_version,capability_kek_write_version,
		capability_comparison_write_version,classification_policy_version,
		cleanup_policy_version,changed_by_hash)
		VALUES('cluster-a',1,'open','open',9,1,1,1,1,1,1,$1)
		ON CONFLICT(cluster_id) DO NOTHING`, actor[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO system_lookup_read_versions VALUES('cluster-a',1) ON CONFLICT DO NOTHING;
		INSERT INTO system_digest_read_versions VALUES('cluster-a',1) ON CONFLICT DO NOTHING;
		INSERT INTO capability_manifests(manifest_id,manifest_version,image_digest,proxy_digest,
		supported_routes,supported_fields,response_parsers,identity_profile,apc_isolation_mode,
		termination_capabilities,signature_algorithm,signature_key_version,signature,valid_from,valid_until)
		VALUES('functional',1,'sha256:image','sha256:proxy','[]','{}','[]','{}','disabled',
		'{}','test',1,decode('01','hex'),'-infinity','infinity') ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	reset := func(t *testing.T) {
		t.Helper()
		if _, err := pool.Exec(ctx, `DELETE FROM orphaned_capacity_debts;
			DELETE FROM admission_grants; DELETE FROM reservations; DELETE FROM request_records;
			DELETE FROM drain_intents; DELETE FROM instance_projections;
			DELETE FROM source_observations; DELETE FROM instance_capacity; DELETE FROM tenant_counters;
			UPDATE system_admission_state SET admission_state='open',dispatch_state='open',
			  fenced_at=NULL,fenced_by_hash=NULL,reopened_at=transaction_timestamp()
			  WHERE cluster_id='cluster-a'`); err != nil {
			t.Fatal(err)
		}
		insertTenant(t, pool, "tenant-a", 2)
	}
	seed := func(t *testing.T, id domain.WorkloadIdentity) {
		t.Helper()
		model := sha256.Sum256([]byte("model-a"))
		config := sha256.Sum256([]byte("config"))
		member := sha256.Sum256([]byte(id.PodUID()))
		if _, err := pool.Exec(ctx, `INSERT INTO instance_capacity(
			cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,
			physical_slots,admission_limit,projection_version)
			VALUES($1,$2,$3,$4,$5,$6,2,2,1)`,
			id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(),
			id.EndpointEpoch(), id.RecoveryEpoch()); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO instance_projections(
			cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,
			normalized_proxy_endpoint,model_fingerprint,config_fingerprint,membership_fingerprint,
			capability_manifest_id,capability_manifest_version,source_stamps,health,projection_version)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'functional',1,'{}','healthy',1)`,
			id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(),
			id.EndpointEpoch(), id.RecoveryEpoch(), "https://"+id.PodUID()+".invalid",
			model[:], config[:], member[:]); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("same-attempt capability recovery and last slot", func(t *testing.T) {
		reset(t)
		id := testIdentity(t, "pod-last", 1)
		seed(t, id)
		cmd := testScheduleCommand(t, "request-last", "attempt-last", "tenant-a")
		reservation, err := store.TryReserve(ctx, cmd, id)
		if err != nil {
			t.Fatal(err)
		}
		recovered, found, err := store.LookupReservation(ctx, cmd)
		if err != nil || !found || recovered.Ref().ID() != reservation.Ref().ID() ||
			!bytes.Equal(recovered.Ref().Capability(), reservation.Ref().Capability()) {
			t.Fatalf("same-attempt recovery mismatch: found=%v err=%v", found, err)
		}
		if _, err := store.TryReserve(ctx, testScheduleCommand(t, "request-loser", "attempt-loser", "tenant-a"), id); !errors.Is(err, domain.ErrNoCapacity) {
			t.Fatalf("last-slot loser: %v", err)
		}
	})

	t.Run("exact drain row blocks reserve", func(t *testing.T) {
		reset(t)
		id := testIdentity(t, "pod-drain", 1)
		seed(t, id)
		if _, err := pool.Exec(ctx, `INSERT INTO drain_intents(
			drain_id,cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,
			scope_kind,state,reason,writer_generation)
			VALUES('drain-functional',$1,$2,$3,$4,$5,$6,'exact_identity','active','test',1)`,
			id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(),
			id.EndpointEpoch(), id.RecoveryEpoch()); err != nil {
			t.Fatal(err)
		}
		if _, err := store.TryReserve(ctx, testScheduleCommand(t, "request-drain", "attempt-drain", "tenant-a"), id); !errors.Is(err, domain.ErrNoCapacity) {
			t.Fatalf("reserve crossed drain: %v", err)
		}
	})

	t.Run("retained grant give-up and sweep", func(t *testing.T) {
		reset(t)
		id := testIdentity(t, "pod-retained", 1)
		seed(t, id)
		cmd := testScheduleCommand(t, "request-retained", "attempt-retained", "tenant-a")
		reservation, err := store.TryReserve(ctx, cmd, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.AbandonBeforeDispatch(ctx, reservation.Ref(), domain.RerankStaleTarget); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE request_records SET classification_after=transaction_timestamp()-interval '1 second'
			WHERE request_id=$1`, cmd.RequestID()); err != nil {
			t.Fatal(err)
		}
		if result, err := store.SweepReservationStates(ctx, 10); err != nil || result.GivenUp != 1 {
			t.Fatalf("retained sweep: result=%+v err=%v", result, err)
		}
	})

	t.Run("ambiguity debt persists and recovery fence closes admission and dispatch", func(t *testing.T) {
		reset(t)
		id := testIdentity(t, "pod-debt", 1)
		seed(t, id)
		cmd := testScheduleCommand(t, "request-debt", "attempt-debt", "tenant-a")
		reservation, err := store.TryReserve(ctx, cmd, id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.PrepareDispatch(ctx, reservation.Ref()); err != nil {
			t.Fatal(err)
		}
		if err := store.MarkAmbiguous(ctx, reservation.Ref(), domain.AmbiguousTransport); err != nil {
			t.Fatal(err)
		}
		var debts int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM orphaned_capacity_debts
			WHERE reservation_id=$1 AND state='active'`, reservation.Ref().ID()).Scan(&debts); err != nil || debts != 1 {
			t.Fatalf("authoritative debt missing: count=%d err=%v", debts, err)
		}
		if _, err := pool.Exec(ctx, `UPDATE system_admission_state SET admission_state='fenced',
			dispatch_state='fenced',fenced_at=transaction_timestamp(),fenced_by_hash=$2
			WHERE cluster_id=$1`, id.Cluster(), actor[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := store.TryReserve(ctx, testScheduleCommand(t, "request-fenced", "attempt-fenced", "tenant-a"), id); !errors.Is(err, domain.ErrInvalidState) {
			t.Fatalf("recovery admission fence: %v", err)
		}
	})

	t.Run("envelope key rotation retains old reads and writes current version", func(t *testing.T) {
		reset(t)
		id := testIdentity(t, "pod-rotation", 1)
		seed(t, id)
		keyring, err := postgresadapter.NewLocalCapabilityKeyring(
			map[uint32][]byte{1: []byte(testCapabilityKey), 2: []byte("abcdef0123456789abcdef0123456789")},
			map[uint32][]byte{1: []byte(testCapabilityKey), 2: []byte("fedcba9876543210fedcba9876543210")},
		)
		if err != nil {
			t.Fatal(err)
		}
		catalog, err := postgresadapter.NewCatalog(pool, keyring)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE system_admission_state
			SET digest_write_version=2,capability_kek_write_version=2,
			    capability_comparison_write_version=2
			WHERE cluster_id='cluster-a';
			INSERT INTO system_digest_read_versions VALUES('cluster-a',2) ON CONFLICT DO NOTHING`); err != nil {
			t.Fatal(err)
		}
		digest1Value := sha256.Sum256([]byte("rotation-v1"))
		digest2Value := sha256.Sum256([]byte("rotation-v2"))
		digest1, _ := domain.NewRequestDigestCandidate(1, digest1Value[:])
		digest2, _ := domain.NewRequestDigestCandidate(2, digest2Value[:])
		cmd, err := domain.NewScheduleCommand(domain.ScheduleParams{
			RequestID: "request-rotation", AttemptID: "attempt-rotation", Tenant: "tenant-a",
			DigestCandidates: []domain.RequestDigestCandidate{digest1, digest2}, DigestWriteVersion: 2,
			Model: "model-a", SlotCost: 2, ExecutionBudget: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		reservation, err := catalog.SchedulerStore().TryReserve(ctx, cmd, id)
		if err != nil {
			t.Fatal(err)
		}
		var kekVersion, comparisonVersion uint32
		if err := pool.QueryRow(ctx, `SELECT capability_kek_version,
			capability_comparison_version FROM reservations WHERE reservation_id=$1`,
			reservation.Ref().ID()).Scan(&kekVersion, &comparisonVersion); err != nil ||
			kekVersion != 2 || comparisonVersion != 2 {
			t.Fatalf("capability rotation versions: kek=%d comparison=%d err=%v",
				kekVersion, comparisonVersion, err)
		}
	})
}

func TestCapacityDebtMigrationBackfill(t *testing.T) {
	root := repositoryRoot(t)
	baseMigrations := make([]string, 0, 5)
	for version := 1; version <= 5; version++ {
		matches, err := filepath.Glob(filepath.Join(root, "migrations", fmt.Sprintf("%06d_*.up.sql", version)))
		if err != nil {
			t.Fatalf("find migration %06d: %v", version, err)
		}
		if len(matches) != 1 {
			t.Fatalf("find migration %06d: got %d files, want 1", version, len(matches))
		}
		baseMigrations = append(baseMigrations, matches[0])
	}
	debtMigration := filepath.Join(root, "migrations", "000006_capacity_debts.up.sql")

	t.Run("consistent orphan", func(t *testing.T) {
		pool := startPostgres(t, baseMigrations)
		seedLegacyOrphan(t, pool, true)
		if err := executeMigration(pool, debtMigration); err != nil {
			t.Fatalf("execute capacity debt migration: %v", err)
		}

		var (
			state, cause, reservationID     string
			slotCost                        int
			backendOrphaned, tenantOrphaned int
		)
		err := pool.QueryRow(context.Background(), `SELECT state, cause, reservation_id, slot_cost
			FROM capacity_debts WHERE debt_id = 'debt_legacy-reservation'`).
			Scan(&state, &cause, &reservationID, &slotCost)
		if err != nil {
			t.Fatalf("read backfilled debt: %v", err)
		}
		if state != "active" || cause != "legacy_orphan" || reservationID != "legacy-reservation" || slotCost != 2 {
			t.Fatalf("backfilled debt = state %q cause %q reservation %q slots %d", state, cause, reservationID, slotCost)
		}
		if err := pool.QueryRow(context.Background(), `SELECT orphaned_slots FROM scheduler_backends WHERE pod_uid='legacy-pod'`).Scan(&backendOrphaned); err != nil {
			t.Fatalf("read backend orphaned slots: %v", err)
		}
		if err := pool.QueryRow(context.Background(), `SELECT orphaned_grants FROM tenant_counters`).Scan(&tenantOrphaned); err != nil {
			t.Fatalf("read tenant orphaned grants: %v", err)
		}
		if backendOrphaned != 2 || tenantOrphaned != 2 {
			t.Fatalf("migration released counters: backend=%d tenant=%d", backendOrphaned, tenantOrphaned)
		}
	})

	t.Run("inconsistent orphan", func(t *testing.T) {
		pool := startPostgres(t, baseMigrations)
		seedLegacyOrphan(t, pool, false)
		err := executeMigration(pool, debtMigration)
		if err == nil || !strings.Contains(err.Error(), "capacity debt backfill found inconsistent orphan accounting") {
			t.Fatalf("migration error = %v, want inconsistent orphan accounting", err)
		}

		var relationPresent bool
		if err := pool.QueryRow(context.Background(), `SELECT to_regclass('public.capacity_debts') IS NOT NULL`).Scan(&relationPresent); err != nil {
			t.Fatalf("check migration rollback: %v", err)
		}
		if relationPresent {
			t.Fatal("capacity_debts remains after failed migration")
		}
	})
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate functional test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
}

func startPostgres(t *testing.T, migrations []string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	root := repositoryRoot(t)
	image := postgresImage(t, root)
	container, err := tcpostgres.Run(ctx, image,
		tcpostgres.WithDatabase("prudentia"),
		tcpostgres.WithUsername("prudentia"),
		tcpostgres.WithPassword("prudentia"),
		tcpostgres.WithOrderedInitScripts(migrations...),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start PostgreSQL %q: %v", image, err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})

	connectionString, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("PostgreSQL connection string: %v", err)
	}
	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		t.Fatalf("create PostgreSQL pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	return pool
}

func executeMigration(pool *pgxpool.Pool, path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read migration: %w", err)
	}
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(context.Background(), string(content)); err != nil {
		_, _ = conn.Exec(context.Background(), "ROLLBACK")
		return err
	}
	return nil
}

func seedLegacyOrphan(t *testing.T, pool *pgxpool.Pool, withGrant bool) {
	t.Helper()
	ctx := context.Background()
	tenantHash := sha256.Sum256([]byte("legacy-tenant"))
	commandHash := sha256.Sum256([]byte("legacy-command"))
	capabilityHash := sha256.Sum256([]byte("legacy-capability"))
	if _, err := pool.Exec(ctx, `INSERT INTO tenant_counters
		(tenant_hash, grant_limit, active_grants, orphaned_grants)
		VALUES ($1, 4, 0, 2)`, tenantHash[:]); err != nil {
		t.Fatalf("seed legacy tenant: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO scheduler_backends (
		cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch,
		model, endpoint, configured_slots, admission_limit, reserved_slots, orphaned_slots,
		drain_active, healthy, eligible_until)
		VALUES ('cluster-a','inference','engine-a','legacy-pod',1,1,
		'model-a','https://legacy-pod.invalid',4,4,0,2,false,true,clock_timestamp()+interval '1 hour')`); err != nil {
		t.Fatalf("seed legacy backend: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO scheduler_reservations (
		reservation_id, request_id, attempt_id, command_hash, tenant_hash, model, slot_cost,
		request_generation, cluster, namespace, logical_engine, pod_uid, endpoint_epoch,
		recovery_epoch, state, capability_ciphertext, capability_hash, execution_deadline)
		VALUES ('legacy-reservation','legacy-request','legacy-attempt',$1,$2,'model-a',2,
		1,'cluster-a','inference','engine-a','legacy-pod',1,1,'orphaned',$3,$4,
		clock_timestamp()+interval '1 hour')`,
		commandHash[:], tenantHash[:], []byte("ciphertext"), capabilityHash[:]); err != nil {
		t.Fatalf("seed legacy reservation: %v", err)
	}
	if withGrant {
		if _, err := pool.Exec(ctx, `INSERT INTO admission_grants (
			grant_id, request_id, reservation_id, tenant_hash, slot_cost, state,
			execution_deadline, classification_after)
			VALUES ('grant_legacy-request','legacy-request','legacy-reservation',$1,2,'orphaned',
			clock_timestamp()+interval '1 hour',clock_timestamp()+interval '2 hours')`, tenantHash[:]); err != nil {
			t.Fatalf("seed legacy grant: %v", err)
		}
	}
}

func postgresImage(t *testing.T, root string) string {
	t.Helper()
	if override := strings.TrimSpace(os.Getenv("PRUDENTIA_TEST_POSTGRES_IMAGE")); override != "" {
		return override
	}
	content, err := os.ReadFile(filepath.Join(root, ".github", "versions.env"))
	if err != nil {
		t.Fatalf("read .github/versions.env: %v", err)
	}
	for _, line := range strings.Split(string(content), "\n") {
		key, value, found := strings.Cut(strings.TrimSpace(line), "=")
		if found && key == "POSTGRES_IMAGE" {
			if image := strings.TrimSpace(value); image != "" {
				return image
			}
			t.Fatal("read .github/versions.env: POSTGRES_IMAGE is empty")
		}
	}
	t.Fatal("read .github/versions.env: POSTGRES_IMAGE is missing")
	return ""
}

func testScheduleCommand(t *testing.T, requestID, attemptID, tenant string) domain.ScheduleCommand {
	t.Helper()
	digestValue := sha256.Sum256([]byte(requestID + "\x00" + tenant))
	digest, err := domain.NewRequestDigestCandidate(1, digestValue[:])
	if err != nil {
		t.Fatal(err)
	}
	command, err := domain.NewScheduleCommand(domain.ScheduleParams{
		RequestID: requestID, AttemptID: attemptID, Tenant: tenant,
		DigestCandidates: []domain.RequestDigestCandidate{digest}, DigestWriteVersion: 1,
		Model: "model-a", SlotCost: 2, ExecutionBudget: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("create schedule command: %v", err)
	}
	return command
}

func testIdempotentScheduleCommand(t *testing.T, requestID, attemptID, tenant string) domain.ScheduleCommand {
	t.Helper()
	lookupValue := sha256.Sum256([]byte("functional/idempotency-lookup"))
	digestValue := sha256.Sum256([]byte("functional/request-digest"))
	lookup, err := domain.NewIdempotencyLookupCandidate(1, lookupValue[:])
	if err != nil {
		t.Fatalf("create idempotency lookup candidate: %v", err)
	}
	digest, err := domain.NewRequestDigestCandidate(1, digestValue[:])
	if err != nil {
		t.Fatalf("create request digest candidate: %v", err)
	}
	command, err := domain.NewScheduleCommand(domain.ScheduleParams{
		RequestID: requestID, AttemptID: attemptID, Tenant: tenant,
		IdempotencyCandidates: []domain.IdempotencyLookupCandidate{lookup}, LookupWriteVersion: 1,
		DigestCandidates: []domain.RequestDigestCandidate{digest}, DigestWriteVersion: 1,
		Model: "model-a", SlotCost: 2, ExecutionBudget: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("create idempotent schedule command: %v", err)
	}
	return command
}

func testIdentity(t *testing.T, podUID string, endpointEpoch uint64) domain.WorkloadIdentity {
	t.Helper()
	identity, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{
		Cluster: "cluster-a", Namespace: "inference", LogicalEngine: "engine-a",
		PodUID: podUID, EndpointEpoch: endpointEpoch, RecoveryEpoch: 1,
	})
	if err != nil {
		t.Fatalf("create workload identity: %v", err)
	}
	return identity
}

func resetSchedulerLedger(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `TRUNCATE capacity_debts, admission_grants, scheduler_reservations, scheduler_backends, tenant_counters`); err != nil {
		t.Fatalf("reset scheduler ledger: %v", err)
	}
	for _, tenant := range []string{"tenant-a", "tenant-b", "tenant-retained"} {
		insertTenant(t, pool, tenant, 100)
	}
}

func insertTenant(t *testing.T, pool *pgxpool.Pool, tenant string, grantLimit int) {
	t.Helper()
	tenantHash := sha256.Sum256([]byte(tenant))
	if _, err := pool.Exec(context.Background(), `INSERT INTO tenant_counters (tenant_hash, grant_limit)
		VALUES ($1,$2) ON CONFLICT (tenant_hash) DO UPDATE SET grant_limit=EXCLUDED.grant_limit`,
		tenantHash[:], grantLimit); err != nil {
		t.Fatalf("insert tenant %q: %v", tenant, err)
	}
}

func insertBackend(t *testing.T, pool *pgxpool.Pool, identity domain.WorkloadIdentity) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `INSERT INTO scheduler_backends (
		cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch,
		model, endpoint, configured_slots, admission_limit, healthy, drain_active, eligible_until)
		VALUES ($1,$2,$3,$4,$5,$6,'model-a',$7,4,4,true,false,clock_timestamp()+interval '1 hour')`,
		identity.Cluster(), identity.Namespace(), identity.LogicalEngine(), identity.PodUID(),
		identity.EndpointEpoch(), identity.RecoveryEpoch(), "https://"+identity.PodUID()+".invalid")
	if err != nil {
		t.Fatalf("insert backend %q: %v", identity.PodUID(), err)
	}
}

func readReservation(t *testing.T, pool *pgxpool.Pool, reservationID string) reservationLedgerRow {
	t.Helper()
	var row reservationLedgerRow
	err := pool.QueryRow(context.Background(), `SELECT state, request_generation, capability_hash,
		execution_deadline, pod_uid FROM scheduler_reservations WHERE reservation_id=$1`, reservationID).
		Scan(&row.state, &row.requestGeneration, &row.capabilityHash, &row.executionDeadline, &row.podUID)
	if err != nil {
		t.Fatalf("read reservation %q: %v", reservationID, err)
	}
	return row
}

func assertReservation(t *testing.T, row reservationLedgerRow, state string, generation uint64, podUID string, ref domain.ReservationRef) {
	t.Helper()
	if row.state != state || row.requestGeneration != generation || row.podUID != podUID {
		t.Fatalf("reservation row: got state=%q generation=%d pod_uid=%q, want state=%q generation=%d pod_uid=%q",
			row.state, row.requestGeneration, row.podUID, state, generation, podUID)
	}
	if !domain.CapabilityMatches(ref.Capability(), row.capabilityHash) {
		t.Fatal("reservation capability does not match persisted hash")
	}
}

func readCapacity(t *testing.T, pool *pgxpool.Pool, identity domain.WorkloadIdentity) backendCapacity {
	t.Helper()
	var capacity backendCapacity
	err := pool.QueryRow(context.Background(), `SELECT reserved_slots, orphaned_slots
		FROM scheduler_backends
		WHERE cluster=$1 AND namespace=$2 AND logical_engine=$3 AND pod_uid=$4
		  AND endpoint_epoch=$5 AND recovery_epoch=$6`,
		identity.Cluster(), identity.Namespace(), identity.LogicalEngine(), identity.PodUID(),
		identity.EndpointEpoch(), identity.RecoveryEpoch()).
		Scan(&capacity.reserved, &capacity.orphaned)
	if err != nil {
		t.Fatalf("read exact backend %q capacity: %v", identity.PodUID(), err)
	}
	return capacity
}

func assertCapacity(t *testing.T, pool *pgxpool.Pool, identity domain.WorkloadIdentity, want backendCapacity) {
	t.Helper()
	if got := readCapacity(t, pool, identity); got != want {
		t.Fatalf("backend %q capacity: got reserved=%d orphaned=%d, want reserved=%d orphaned=%d",
			identity.PodUID(), got.reserved, got.orphaned, want.reserved, want.orphaned)
	}
}

func assertTenantUsage(t *testing.T, pool *pgxpool.Pool, tenant string, active, orphaned int) {
	t.Helper()
	tenantHash := sha256.Sum256([]byte(tenant))
	var gotActive, gotOrphaned int
	if err := pool.QueryRow(context.Background(), `SELECT active_grants, orphaned_grants
		FROM tenant_counters WHERE tenant_hash=$1`, tenantHash[:]).Scan(&gotActive, &gotOrphaned); err != nil {
		t.Fatalf("read tenant %q usage: %v", tenant, err)
	}
	if gotActive != active || gotOrphaned != orphaned {
		t.Fatalf("tenant %q usage: got active=%d orphaned=%d, want active=%d orphaned=%d",
			tenant, gotActive, gotOrphaned, active, orphaned)
	}
}

func assertGrantState(t *testing.T, pool *pgxpool.Pool, reservationID, want string) {
	t.Helper()
	var state string
	var activeContribution, orphanedContribution int
	if err := pool.QueryRow(context.Background(), `SELECT state, active_contribution, orphaned_contribution
		FROM admission_grants WHERE reservation_id=$1`, reservationID).
		Scan(&state, &activeContribution, &orphanedContribution); err != nil {
		t.Fatalf("read admission grant for %q: %v", reservationID, err)
	}
	if state != want {
		t.Fatalf("admission grant state: got %q, want %q", state, want)
	}
	contribution := activeContribution + orphanedContribution
	if want == "released" && contribution != 0 {
		t.Fatalf("released grant contribution: got %d, want 0", contribution)
	}
	if want != "released" && contribution != 2 {
		t.Fatalf("contributing grant total: got %d, want 2", contribution)
	}
}

func assertActiveDebt(t *testing.T, pool *pgxpool.Pool, reservationID string, identity domain.WorkloadIdentity, tenant, cause string, slotCost int) {
	t.Helper()
	var (
		debtID, state, gotCause string
		tenantHash              []byte
		identityParams          domain.WorkloadIdentityParams
		gotSlotCost             int
	)
	err := pool.QueryRow(context.Background(), `SELECT debt_id, state, cause, tenant_hash,
		cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch, slot_cost
		FROM capacity_debts WHERE reservation_id=$1`, reservationID).
		Scan(&debtID, &state, &gotCause, &tenantHash, &identityParams.Cluster,
			&identityParams.Namespace, &identityParams.LogicalEngine, &identityParams.PodUID,
			&identityParams.EndpointEpoch, &identityParams.RecoveryEpoch, &gotSlotCost)
	if err != nil {
		t.Fatalf("read debt for reservation %q: %v", reservationID, err)
	}
	gotIdentity, err := domain.NewWorkloadIdentity(identityParams)
	if err != nil {
		t.Fatalf("decode debt identity for reservation %q: %v", reservationID, err)
	}
	wantTenantHash := sha256.Sum256([]byte(tenant))
	if debtID != "debt_"+reservationID || state != "active" || gotCause != cause ||
		!bytes.Equal(tenantHash, wantTenantHash[:]) || !gotIdentity.Equal(identity) ||
		gotSlotCost != slotCost {
		t.Fatalf("capacity debt binding mismatch for reservation %q", reservationID)
	}
}

func testDebtResolution(t *testing.T, reservationID string, identity domain.WorkloadIdentity, generation domain.WriterGeneration, label string) domain.DebtResolution {
	t.Helper()
	return testDebtResolutionFor(t, "debt_"+reservationID, reservationID, identity, generation, label)
}

func testDebtResolutionFor(t *testing.T, debtID, reservationID string, identity domain.WorkloadIdentity, generation domain.WriterGeneration, label string) domain.DebtResolution {
	t.Helper()
	proof, err := domain.NewIdentityGoneProof(domain.IdentityGoneProofParams{
		WriterGeneration:               generation,
		Identity:                       identity,
		PodAbsenceEvidenceHash:         sha256.Sum256([]byte(label + "/pod-absence")),
		EndpointWithdrawalEvidenceHash: sha256.Sum256([]byte(label + "/endpoint-withdrawal")),
		ExecutionFenceEvidenceHash:     sha256.Sum256([]byte(label + "/execution-fence")),
	})
	if err != nil {
		t.Fatalf("create identity-gone proof: %v", err)
	}
	resolution, err := domain.NewIdentityGoneDebtResolution(debtID, reservationID, proof)
	if err != nil {
		t.Fatalf("create debt resolution: %v", err)
	}
	return resolution
}

func alteredIdentity(t *testing.T, base domain.WorkloadIdentity, namespace, podUID string, endpointEpoch, recoveryEpoch uint64) domain.WorkloadIdentity {
	t.Helper()
	identity, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{
		Cluster: base.Cluster(), Namespace: namespace, LogicalEngine: base.LogicalEngine(),
		PodUID: podUID, EndpointEpoch: endpointEpoch, RecoveryEpoch: recoveryEpoch,
	})
	if err != nil {
		t.Fatalf("create altered workload identity: %v", err)
	}
	return identity
}

func assertResolvedDebt(t *testing.T, pool *pgxpool.Pool, reservationID string, evidenceHash [32]byte) {
	t.Helper()
	var (
		state      string
		storedHash []byte
		resolved   bool
	)
	err := pool.QueryRow(context.Background(), `SELECT state, resolution_evidence_hash, resolved_at IS NOT NULL
		FROM capacity_debts WHERE reservation_id=$1`, reservationID).Scan(&state, &storedHash, &resolved)
	if err != nil {
		t.Fatalf("read resolved debt for reservation %q: %v", reservationID, err)
	}
	if state != "resolved_identity_gone" || !bytes.Equal(storedHash, evidenceHash[:]) || !resolved {
		t.Fatalf("resolved debt mismatch for reservation %q: state=%q hash=%x resolved=%t", reservationID, state, storedHash, resolved)
	}
}

func capacityDebtCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM capacity_debts`).Scan(&count); err != nil {
		t.Fatalf("count capacity debts: %v", err)
	}
	return count
}

func admissionGrantCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM admission_grants`).Scan(&count); err != nil {
		t.Fatalf("count admission grants: %v", err)
	}
	return count
}

func assertEmptyContributions(t *testing.T, pool *pgxpool.Pool, identity domain.WorkloadIdentity, tenant string) {
	t.Helper()
	assertCapacity(t, pool, identity, backendCapacity{})
	assertTenantUsage(t, pool, tenant, 0, 0)
	if count := admissionGrantCount(t, pool); count != 0 {
		t.Fatalf("admission grant count after rollback: got %d, want 0", count)
	}
	var reservations int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM scheduler_reservations`).Scan(&reservations); err != nil {
		t.Fatalf("count reservations after rollback: %v", err)
	}
	if reservations != 0 {
		t.Fatalf("reservation count after rollback: got %d, want 0", reservations)
	}
}

func reservationCount(t *testing.T, pool *pgxpool.Pool, requestID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM scheduler_reservations WHERE request_id=$1`, requestID).Scan(&count); err != nil {
		t.Fatalf("count reservations for %q: %v", requestID, err)
	}
	return count
}
