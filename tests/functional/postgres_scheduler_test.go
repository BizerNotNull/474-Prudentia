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
	"sync"
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

func TestPreDispatchRerankTransactions(t *testing.T) {
	ctx := context.Background()
	root := repositoryRoot(t)
	migrations, err := filepath.Glob(filepath.Join(root, "migrations", "*.up.sql"))
	if err != nil {
		t.Fatalf("find migrations: %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("find migrations: no up migrations")
	}
	sort.Strings(migrations)

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
	store, err := postgresadapter.NewSchedulerStore(pool, []byte(testCapabilityKey))
	if err != nil {
		t.Fatalf("create scheduler store: %v", err)
	}

	t.Run("stale candidate reranks with generation fencing", func(t *testing.T) {
		resetSchedulerLedger(t, pool)
		backendA := testIdentity(t, "pod-a", 1)
		backendB := testIdentity(t, "pod-b", 1)
		insertBackend(t, pool, backendA)
		insertBackend(t, pool, backendB)
		command := testScheduleCommand(t, "request-stale", "attempt-stale", "tenant-a")

		first, err := store.TryReserve(ctx, command, backendA)
		if err != nil {
			t.Fatalf("reserve backend A: %v", err)
		}
		ref1 := first.Ref()
		row1 := readReservation(t, pool, ref1.ID())
		assertReservation(t, row1, "reserved", 1, "pod-a", ref1)
		assertCapacity(t, pool, "pod-a", backendCapacity{reserved: 2})

		if _, err := pool.Exec(ctx, `UPDATE scheduler_backends SET healthy=false WHERE pod_uid=$1`, "pod-a"); err != nil {
			t.Fatalf("mark backend A unhealthy: %v", err)
		}
		if _, err := store.PrepareDispatch(ctx, ref1); !errors.Is(err, domain.ErrStaleTarget) {
			t.Fatalf("prepare stale backend: got %v, want %v", err, domain.ErrStaleTarget)
		}
		assertReservation(t, readReservation(t, pool, ref1.ID()), "reserved", 1, "pod-a", ref1)
		assertCapacity(t, pool, "pod-a", backendCapacity{reserved: 2})

		if err := store.AbandonBeforeDispatch(ctx, ref1, domain.RerankStaleTarget); err != nil {
			t.Fatalf("abandon stale backend: %v", err)
		}
		assertReservation(t, readReservation(t, pool, ref1.ID()), "abandoned_rerank", 1, "pod-a", ref1)
		assertCapacity(t, pool, "pod-a", backendCapacity{})
		if err := store.AbandonBeforeDispatch(ctx, ref1, domain.RerankStaleTarget); err != nil {
			t.Fatalf("repeat stale abandonment: %v", err)
		}
		assertCapacity(t, pool, "pod-a", backendCapacity{})

		second, err := store.TryReserve(ctx, command, backendB)
		if err != nil {
			t.Fatalf("rerank to backend B: %v", err)
		}
		ref2 := second.Ref()
		row2 := readReservation(t, pool, ref2.ID())
		if ref2.ID() != ref1.ID() {
			t.Fatalf("rerank reservation ID changed: got %q, want %q", ref2.ID(), ref1.ID())
		}
		if count := reservationCount(t, pool, command.RequestID()); count != 1 {
			t.Fatalf("reservation row count: got %d, want 1", count)
		}
		assertReservation(t, row2, "reserved", 2, "pod-b", ref2)
		if !row2.executionDeadline.Equal(row1.executionDeadline) {
			t.Fatalf("execution deadline reset: got %s, want %s", row2.executionDeadline, row1.executionDeadline)
		}
		if bytes.Equal(ref1.Capability(), ref2.Capability()) {
			t.Fatal("rerank reused the generation 1 capability")
		}
		if bytes.Equal(row1.capabilityHash, row2.capabilityHash) {
			t.Fatal("rerank reused the generation 1 capability hash")
		}
		assertCapacity(t, pool, "pod-a", backendCapacity{})
		assertCapacity(t, pool, "pod-b", backendCapacity{reserved: 2})

		if err := store.GiveUpBeforeDispatch(ctx, ref1, domain.GiveUpReranksExhausted); !errors.Is(err, domain.ErrInvalidReference) {
			t.Fatalf("give up with stale generation: got %v, want %v", err, domain.ErrInvalidReference)
		}
		assertReservation(t, readReservation(t, pool, ref2.ID()), "reserved", 2, "pod-b", ref2)
		assertCapacity(t, pool, "pod-b", backendCapacity{reserved: 2})
		if err := store.GiveUpBeforeDispatch(ctx, ref2, domain.GiveUpReranksExhausted); err != nil {
			t.Fatalf("give up latest generation: %v", err)
		}
		assertReservation(t, readReservation(t, pool, ref2.ID()), "given_up", 2, "pod-b", ref2)
		assertCapacity(t, pool, "pod-b", backendCapacity{})
		if err := store.GiveUpBeforeDispatch(ctx, ref2, domain.GiveUpReranksExhausted); err != nil {
			t.Fatalf("repeat latest give-up: %v", err)
		}
		assertCapacity(t, pool, "pod-b", backendCapacity{})
	})

	t.Run("retained rerank terminal give-up is exact once", func(t *testing.T) {
		cases := []struct {
			name   string
			reason domain.GiveUpReason
		}{
			{name: "canceled", reason: domain.GiveUpCanceled},
			{name: "budget expired", reason: domain.GiveUpBudgetExpired},
			{name: "reranks exhausted concurrently", reason: domain.GiveUpReranksExhausted},
		}
		for index, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				resetSchedulerLedger(t, pool)
				backend := testIdentity(t, fmt.Sprintf("pod-retained-%d", index), 1)
				insertBackend(t, pool, backend)
				command := testScheduleCommand(t, fmt.Sprintf("request-retained-%d", index), fmt.Sprintf("attempt-retained-%d", index), "tenant-retained")
				reservation, err := store.TryReserve(ctx, command, backend)
				if err != nil {
					t.Fatalf("reserve retained rerank: %v", err)
				}
				ref := reservation.Ref()
				if err := store.AbandonBeforeDispatch(ctx, ref, domain.RerankStaleTarget); err != nil {
					t.Fatalf("abandon retained rerank: %v", err)
				}

				if testCase.reason == domain.GiveUpReranksExhausted {
					start := make(chan struct{})
					errorsByCall := make([]error, 2)
					var calls sync.WaitGroup
					calls.Add(2)
					for call := range errorsByCall {
						go func(call int) {
							defer calls.Done()
							<-start
							errorsByCall[call] = store.GiveUpBeforeDispatch(ctx, ref, testCase.reason)
						}(call)
					}
					close(start)
					calls.Wait()
					for call, err := range errorsByCall {
						if err != nil {
							t.Fatalf("concurrent give-up call %d: %v", call, err)
						}
					}
				} else if err := store.GiveUpBeforeDispatch(ctx, ref, testCase.reason); err != nil {
					t.Fatalf("give up retained rerank: %v", err)
				}

				assertReservation(t, readReservation(t, pool, ref.ID()), "given_up", 1, backend.PodUID(), ref)
				if count := reservationCount(t, pool, command.RequestID()); count != 1 {
					t.Fatalf("reservation row count: got %d, want 1", count)
				}
				assertCapacity(t, pool, backend.PodUID(), backendCapacity{})
			})
		}
	})

	t.Run("mismatched rerank command rolls back", func(t *testing.T) {
		resetSchedulerLedger(t, pool)
		backendA := testIdentity(t, "pod-rollback-a", 1)
		backendB := testIdentity(t, "pod-rollback-b", 1)
		insertBackend(t, pool, backendA)
		insertBackend(t, pool, backendB)
		command := testScheduleCommand(t, "request-rollback", "attempt-rollback", "tenant-a")
		reservation, err := store.TryReserve(ctx, command, backendA)
		if err != nil {
			t.Fatalf("reserve rollback fixture: %v", err)
		}
		ref := reservation.Ref()
		if err := store.AbandonBeforeDispatch(ctx, ref, domain.RerankStaleTarget); err != nil {
			t.Fatalf("abandon rollback fixture: %v", err)
		}
		mismatched := testScheduleCommand(t, command.RequestID(), command.AttemptID(), "tenant-b")
		if _, err := store.TryReserve(ctx, mismatched, backendB); !errors.Is(err, domain.ErrInvalidState) {
			t.Fatalf("reserve mismatched command: got %v, want %v", err, domain.ErrInvalidState)
		}
		assertReservation(t, readReservation(t, pool, ref.ID()), "abandoned_rerank", 1, backendA.PodUID(), ref)
		assertCapacity(t, pool, backendA.PodUID(), backendCapacity{})
		assertCapacity(t, pool, backendB.PodUID(), backendCapacity{})
	})

	t.Run("dispatch authorization fails closed", func(t *testing.T) {
		resetSchedulerLedger(t, pool)
		backend := testIdentity(t, "pod-authorized", 1)
		insertBackend(t, pool, backend)
		command := testScheduleCommand(t, "request-authorized", "attempt-authorized", "tenant-a")
		reservation, err := store.TryReserve(ctx, command, backend)
		if err != nil {
			t.Fatalf("reserve authorized fixture: %v", err)
		}
		ref := reservation.Ref()
		if _, err := store.PrepareDispatch(ctx, ref); err != nil {
			t.Fatalf("prepare dispatch: %v", err)
		}
		if err := store.AbandonBeforeDispatch(ctx, ref, domain.RerankStaleTarget); !errors.Is(err, domain.ErrInvalidState) {
			t.Fatalf("abandon authorized reservation: got %v, want %v", err, domain.ErrInvalidState)
		}
		if err := store.GiveUpBeforeDispatch(ctx, ref, domain.GiveUpCanceled); !errors.Is(err, domain.ErrInvalidState) {
			t.Fatalf("give up authorized reservation: got %v, want %v", err, domain.ErrInvalidState)
		}
		assertReservation(t, readReservation(t, pool, ref.ID()), "dispatch_authorized", 1, backend.PodUID(), ref)
		assertCapacity(t, pool, backend.PodUID(), backendCapacity{reserved: 2})
	})

	t.Run("database time classifies expired reservations", func(t *testing.T) {
		resetSchedulerLedger(t, pool)
		reservedBackend := testIdentity(t, "pod-expired-reserved", 1)
		abandonedBackend := testIdentity(t, "pod-expired-abandoned", 1)
		authorizedBackend := testIdentity(t, "pod-expired-authorized", 1)
		for _, backend := range []domain.WorkloadIdentity{reservedBackend, abandonedBackend, authorizedBackend} {
			insertBackend(t, pool, backend)
		}

		reserved, err := store.TryReserve(ctx, testScheduleCommand(t, "request-expired-reserved", "attempt-expired-reserved", "tenant-a"), reservedBackend)
		if err != nil {
			t.Fatalf("reserve expired reserved fixture: %v", err)
		}
		abandoned, err := store.TryReserve(ctx, testScheduleCommand(t, "request-expired-abandoned", "attempt-expired-abandoned", "tenant-a"), abandonedBackend)
		if err != nil {
			t.Fatalf("reserve expired abandoned fixture: %v", err)
		}
		if err := store.AbandonBeforeDispatch(ctx, abandoned.Ref(), domain.RerankStaleTarget); err != nil {
			t.Fatalf("abandon expired fixture: %v", err)
		}
		authorized, err := store.TryReserve(ctx, testScheduleCommand(t, "request-expired-authorized", "attempt-expired-authorized", "tenant-a"), authorizedBackend)
		if err != nil {
			t.Fatalf("reserve expired authorized fixture: %v", err)
		}
		if _, err := store.PrepareDispatch(ctx, authorized.Ref()); err != nil {
			t.Fatalf("authorize expired fixture: %v", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE scheduler_reservations SET execution_deadline=clock_timestamp()-interval '1 second'`); err != nil {
			t.Fatalf("expire reservations: %v", err)
		}

		classified, err := store.ClassifyExpired(ctx, 10)
		if err != nil {
			t.Fatalf("classify expired reservations: %v", err)
		}
		if classified != 3 {
			t.Fatalf("classified reservations: got %d, want 3", classified)
		}
		assertReservation(t, readReservation(t, pool, reserved.Ref().ID()), "given_up", 1, reservedBackend.PodUID(), reserved.Ref())
		assertReservation(t, readReservation(t, pool, abandoned.Ref().ID()), "given_up", 1, abandonedBackend.PodUID(), abandoned.Ref())
		assertReservation(t, readReservation(t, pool, authorized.Ref().ID()), "orphaned", 1, authorizedBackend.PodUID(), authorized.Ref())
		assertCapacity(t, pool, reservedBackend.PodUID(), backendCapacity{})
		assertCapacity(t, pool, abandonedBackend.PodUID(), backendCapacity{})
		assertCapacity(t, pool, authorizedBackend.PodUID(), backendCapacity{orphaned: 2})

		classified, err = store.ClassifyExpired(ctx, 10)
		if err != nil {
			t.Fatalf("repeat expired classification: %v", err)
		}
		if classified != 0 {
			t.Fatalf("repeat classified reservations: got %d, want 0", classified)
		}
		assertCapacity(t, pool, reservedBackend.PodUID(), backendCapacity{})
		assertCapacity(t, pool, abandonedBackend.PodUID(), backendCapacity{})
		assertCapacity(t, pool, authorizedBackend.PodUID(), backendCapacity{orphaned: 2})
	})

	t.Run("tenant last grant is serialized across scheduler replicas", func(t *testing.T) {
		resetSchedulerLedger(t, pool)
		insertTenant(t, pool, "tenant-contended", 2)
		backendA := testIdentity(t, "pod-tenant-contended-a", 1)
		backendB := testIdentity(t, "pod-tenant-contended-b", 1)
		insertBackend(t, pool, backendA)
		insertBackend(t, pool, backendB)
		replicaB, err := postgresadapter.NewSchedulerStore(pool, []byte(testCapabilityKey))
		if err != nil {
			t.Fatalf("create second scheduler store: %v", err)
		}
		commands := []domain.ScheduleCommand{
			testScheduleCommand(t, "request-tenant-contended-a", "attempt-tenant-contended-a", "tenant-contended"),
			testScheduleCommand(t, "request-tenant-contended-b", "attempt-tenant-contended-b", "tenant-contended"),
		}
		stores := []*postgresadapter.SchedulerStore{store, replicaB}
		backends := []domain.WorkloadIdentity{backendA, backendB}
		start := make(chan struct{})
		results := make([]error, 2)
		var calls sync.WaitGroup
		calls.Add(2)
		for call := range results {
			go func(call int) {
				defer calls.Done()
				<-start
				_, results[call] = stores[call].TryReserve(ctx, commands[call], backends[call])
			}(call)
		}
		close(start)
		calls.Wait()
		successes := 0
		for _, err := range results {
			if err == nil {
				successes++
			}
		}
		if successes != 1 {
			t.Fatalf("successful tenant grant contenders: got %d, want 1; errors=%v", successes, results)
		}
		assertTenantUsage(t, pool, "tenant-contended", 2, 0)
		if count := admissionGrantCount(t, pool); count != 1 {
			t.Fatalf("admission grant count: got %d, want 1", count)
		}
	})

	t.Run("backend last slot is not oversold", func(t *testing.T) {
		resetSchedulerLedger(t, pool)
		backend := testIdentity(t, "pod-slot-contended", 1)
		insertBackend(t, pool, backend)
		if _, err := pool.Exec(ctx, `UPDATE scheduler_backends SET admission_limit=2 WHERE pod_uid=$1`, backend.PodUID()); err != nil {
			t.Fatalf("set last-slot capacity: %v", err)
		}
		replicaB, err := postgresadapter.NewSchedulerStore(pool, []byte(testCapabilityKey))
		if err != nil {
			t.Fatalf("create second scheduler store: %v", err)
		}
		commands := []domain.ScheduleCommand{
			testScheduleCommand(t, "request-slot-contended-a", "attempt-slot-contended-a", "tenant-a"),
			testScheduleCommand(t, "request-slot-contended-b", "attempt-slot-contended-b", "tenant-b"),
		}
		stores := []*postgresadapter.SchedulerStore{store, replicaB}
		start := make(chan struct{})
		results := make([]error, 2)
		var calls sync.WaitGroup
		calls.Add(2)
		for call := range results {
			go func(call int) {
				defer calls.Done()
				<-start
				_, results[call] = stores[call].TryReserve(ctx, commands[call], backend)
			}(call)
		}
		close(start)
		calls.Wait()
		successes := 0
		for _, err := range results {
			if err == nil {
				successes++
			}
		}
		if successes != 1 {
			t.Fatalf("successful backend contenders: got %d, want 1; errors=%v", successes, results)
		}
		assertCapacity(t, pool, backend.PodUID(), backendCapacity{reserved: 2})
		if count := admissionGrantCount(t, pool); count != 1 {
			t.Fatalf("admission grant count: got %d, want 1", count)
		}
	})

	t.Run("rerank reuses one tenant grant across candidates", func(t *testing.T) {
		resetSchedulerLedger(t, pool)
		backends := []domain.WorkloadIdentity{
			testIdentity(t, "pod-rerank-reuse-a", 1),
			testIdentity(t, "pod-rerank-reuse-b", 1),
			testIdentity(t, "pod-rerank-reuse-c", 1),
		}
		for _, backend := range backends {
			insertBackend(t, pool, backend)
		}
		command := testScheduleCommand(t, "request-rerank-reuse", "attempt-rerank-reuse", "tenant-a")
		reservation, err := store.TryReserve(ctx, command, backends[0])
		if err != nil {
			t.Fatalf("initial reserve: %v", err)
		}
		ref := reservation.Ref()
		for index := 1; index < len(backends); index++ {
			if err := store.AbandonBeforeDispatch(ctx, ref, domain.RerankStaleTarget); err != nil {
				t.Fatalf("abandon candidate %d: %v", index, err)
			}
			assertTenantUsage(t, pool, "tenant-a", 2, 0)
			next, err := store.TryReserve(ctx, command, backends[index])
			if err != nil {
				t.Fatalf("reserve candidate %d: %v", index, err)
			}
			ref = next.Ref()
		}
		assertTenantUsage(t, pool, "tenant-a", 2, 0)
		if count := admissionGrantCount(t, pool); count != 1 {
			t.Fatalf("admission grant count: got %d, want 1", count)
		}
		assertGrantState(t, pool, ref.ID(), "active_reserved")
	})

	t.Run("terminal give-up releases tenant and backend exactly once", func(t *testing.T) {
		resetSchedulerLedger(t, pool)
		backend := testIdentity(t, "pod-give-up-exact-once", 1)
		insertBackend(t, pool, backend)
		reservation, err := store.TryReserve(ctx,
			testScheduleCommand(t, "request-give-up-exact-once", "attempt-give-up-exact-once", "tenant-a"), backend)
		if err != nil {
			t.Fatalf("reserve give-up fixture: %v", err)
		}
		ref := reservation.Ref()
		start := make(chan struct{})
		results := make([]error, 4)
		var calls sync.WaitGroup
		calls.Add(len(results))
		for call := range results {
			go func(call int) {
				defer calls.Done()
				<-start
				results[call] = store.GiveUpBeforeDispatch(ctx, ref, domain.GiveUpCanceled)
			}(call)
		}
		close(start)
		calls.Wait()
		for call, err := range results {
			if err != nil {
				t.Fatalf("give-up call %d: %v", call, err)
			}
		}
		assertCapacity(t, pool, backend.PodUID(), backendCapacity{})
		assertTenantUsage(t, pool, "tenant-a", 0, 0)
		assertGrantState(t, pool, ref.ID(), "released")
	})

	t.Run("ambiguous dispatch moves both contributions to orphaned", func(t *testing.T) {
		resetSchedulerLedger(t, pool)
		backend := testIdentity(t, "pod-ambiguous-debt", 1)
		insertBackend(t, pool, backend)
		reservation, err := store.TryReserve(ctx,
			testScheduleCommand(t, "request-ambiguous-debt", "attempt-ambiguous-debt", "tenant-a"), backend)
		if err != nil {
			t.Fatalf("reserve ambiguous fixture: %v", err)
		}
		ref := reservation.Ref()
		if _, err := store.PrepareDispatch(ctx, ref); err != nil {
			t.Fatalf("prepare ambiguous fixture: %v", err)
		}
		if err := store.MarkAmbiguous(ctx, ref, domain.AmbiguousTransport); err != nil {
			t.Fatalf("mark ambiguous: %v", err)
		}
		if err := store.MarkAmbiguous(ctx, ref, domain.AmbiguousTransport); err != nil {
			t.Fatalf("repeat mark ambiguous: %v", err)
		}
		assertCapacity(t, pool, backend.PodUID(), backendCapacity{orphaned: 2})
		assertTenantUsage(t, pool, "tenant-a", 0, 2)
		assertGrantState(t, pool, ref.ID(), "orphaned")
	})

	t.Run("another scheduler replica recovers capability and finalizes", func(t *testing.T) {
		resetSchedulerLedger(t, pool)
		backend := testIdentity(t, "pod-replica-recovery", 1)
		insertBackend(t, pool, backend)
		command := testScheduleCommand(t, "request-replica-recovery", "attempt-replica-recovery", "tenant-a")
		reservation, err := store.TryReserve(ctx, command, backend)
		if err != nil {
			t.Fatalf("reserve on replica A: %v", err)
		}
		replicaB, err := postgresadapter.NewSchedulerStore(pool, []byte(testCapabilityKey))
		if err != nil {
			t.Fatalf("create replica B: %v", err)
		}
		recovered, found, err := replicaB.LookupReservation(ctx, command)
		if err != nil || !found {
			t.Fatalf("recover on replica B: found=%v err=%v", found, err)
		}
		if !bytes.Equal(recovered.Ref().Capability(), reservation.Ref().Capability()) {
			t.Fatal("replica B recovered a different capability")
		}
		if _, err := replicaB.PrepareDispatch(ctx, recovered.Ref()); err != nil {
			t.Fatalf("prepare on replica B: %v", err)
		}
		if err := replicaB.Finalize(ctx, recovered.Ref(), domain.TerminalProofProviderFinish); err != nil {
			t.Fatalf("finalize on replica B: %v", err)
		}
		assertCapacity(t, pool, backend.PodUID(), backendCapacity{})
		assertTenantUsage(t, pool, "tenant-a", 0, 0)
		assertGrantState(t, pool, recovered.Ref().ID(), "released")
	})

	t.Run("failed reservation stages roll back every contribution", func(t *testing.T) {
		t.Run("tenant limit", func(t *testing.T) {
			resetSchedulerLedger(t, pool)
			insertTenant(t, pool, "tenant-denied", 0)
			backend := testIdentity(t, "pod-rollback-tenant", 1)
			insertBackend(t, pool, backend)
			_, err := store.TryReserve(ctx,
				testScheduleCommand(t, "request-rollback-tenant", "attempt-rollback-tenant", "tenant-denied"), backend)
			if !errors.Is(err, domain.ErrNoCapacity) {
				t.Fatalf("tenant-limit reserve: got %v, want %v", err, domain.ErrNoCapacity)
			}
			assertEmptyContributions(t, pool, backend.PodUID(), "tenant-denied")
		})

		t.Run("backend capacity", func(t *testing.T) {
			resetSchedulerLedger(t, pool)
			backend := testIdentity(t, "pod-rollback-capacity", 1)
			insertBackend(t, pool, backend)
			if _, err := pool.Exec(ctx, `UPDATE scheduler_backends SET admission_limit=0 WHERE pod_uid=$1`, backend.PodUID()); err != nil {
				t.Fatalf("close backend admission: %v", err)
			}
			_, err := store.TryReserve(ctx,
				testScheduleCommand(t, "request-rollback-capacity", "attempt-rollback-capacity", "tenant-a"), backend)
			if !errors.Is(err, domain.ErrNoCapacity) {
				t.Fatalf("capacity reserve: got %v, want %v", err, domain.ErrNoCapacity)
			}
			assertEmptyContributions(t, pool, backend.PodUID(), "tenant-a")
		})

		for _, failure := range []struct {
			name  string
			table string
		}{
			{name: "reservation persistence", table: "scheduler_reservations"},
			{name: "grant persistence", table: "admission_grants"},
		} {
			t.Run(failure.name, func(t *testing.T) {
				resetSchedulerLedger(t, pool)
				backend := testIdentity(t, "pod-rollback-"+strings.ReplaceAll(failure.name, " ", "-"), 1)
				insertBackend(t, pool, backend)
				if _, err := pool.Exec(ctx, `CREATE OR REPLACE FUNCTION fail_scheduler_insert() RETURNS trigger
					LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected insert failure'; END $$`); err != nil {
					t.Fatalf("create failure function: %v", err)
				}
				triggerName := "fail_" + strings.ReplaceAll(failure.table, "_", "")
				if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON %s
					FOR EACH ROW EXECUTE FUNCTION fail_scheduler_insert()`, triggerName, failure.table)); err != nil {
					t.Fatalf("create failure trigger: %v", err)
				}
				t.Cleanup(func() {
					_, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP TRIGGER IF EXISTS %s ON %s`, triggerName, failure.table))
					_, _ = pool.Exec(context.Background(), `DROP FUNCTION IF EXISTS fail_scheduler_insert()`)
				})
				_, err := store.TryReserve(ctx,
					testScheduleCommand(t, "request-"+triggerName, "attempt-"+triggerName, "tenant-a"), backend)
				if err == nil {
					t.Fatal("reserve unexpectedly succeeded with injected insert failure")
				}
				assertEmptyContributions(t, pool, backend.PodUID(), "tenant-a")
				if _, err := pool.Exec(ctx, fmt.Sprintf(`DROP TRIGGER %s ON %s`, triggerName, failure.table)); err != nil {
					t.Fatalf("drop failure trigger: %v", err)
				}
				if _, err := pool.Exec(ctx, `DROP FUNCTION fail_scheduler_insert()`); err != nil {
					t.Fatalf("drop failure function: %v", err)
				}
			})
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
	command, err := domain.NewScheduleCommand(domain.ScheduleParams{
		RequestID: requestID, AttemptID: attemptID, Tenant: tenant,
		Model: "model-a", SlotCost: 2, ExecutionBudget: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("create schedule command: %v", err)
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
	if _, err := pool.Exec(context.Background(), `TRUNCATE admission_grants, scheduler_reservations, scheduler_backends, tenant_counters`); err != nil {
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

func readCapacity(t *testing.T, pool *pgxpool.Pool, podUID string) backendCapacity {
	t.Helper()
	var capacity backendCapacity
	if err := pool.QueryRow(context.Background(), `SELECT reserved_slots, orphaned_slots FROM scheduler_backends WHERE pod_uid=$1`, podUID).
		Scan(&capacity.reserved, &capacity.orphaned); err != nil {
		t.Fatalf("read backend %q capacity: %v", podUID, err)
	}
	return capacity
}

func assertCapacity(t *testing.T, pool *pgxpool.Pool, podUID string, want backendCapacity) {
	t.Helper()
	if got := readCapacity(t, pool, podUID); got != want {
		t.Fatalf("backend %q capacity: got reserved=%d orphaned=%d, want reserved=%d orphaned=%d",
			podUID, got.reserved, got.orphaned, want.reserved, want.orphaned)
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

func admissionGrantCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM admission_grants`).Scan(&count); err != nil {
		t.Fatalf("count admission grants: %v", err)
	}
	return count
}

func assertEmptyContributions(t *testing.T, pool *pgxpool.Pool, podUID, tenant string) {
	t.Helper()
	assertCapacity(t, pool, podUID, backendCapacity{})
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
