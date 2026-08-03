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
	pool := startPostgres(t, migrations)
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
		assertCapacity(t, pool, backendA, backendCapacity{reserved: 2})

		if _, err := pool.Exec(ctx, `UPDATE scheduler_backends SET healthy=false WHERE pod_uid=$1`, "pod-a"); err != nil {
			t.Fatalf("mark backend A unhealthy: %v", err)
		}
		if _, err := store.PrepareDispatch(ctx, ref1); !errors.Is(err, domain.ErrStaleTarget) {
			t.Fatalf("prepare stale backend: got %v, want %v", err, domain.ErrStaleTarget)
		}
		assertReservation(t, readReservation(t, pool, ref1.ID()), "reserved", 1, "pod-a", ref1)
		assertCapacity(t, pool, backendA, backendCapacity{reserved: 2})

		if err := store.AbandonBeforeDispatch(ctx, ref1, domain.RerankStaleTarget); err != nil {
			t.Fatalf("abandon stale backend: %v", err)
		}
		assertReservation(t, readReservation(t, pool, ref1.ID()), "abandoned_rerank", 1, "pod-a", ref1)
		assertCapacity(t, pool, backendA, backendCapacity{})
		if err := store.AbandonBeforeDispatch(ctx, ref1, domain.RerankStaleTarget); err != nil {
			t.Fatalf("repeat stale abandonment: %v", err)
		}
		assertCapacity(t, pool, backendA, backendCapacity{})

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
		assertCapacity(t, pool, backendA, backendCapacity{})
		assertCapacity(t, pool, backendB, backendCapacity{reserved: 2})

		if err := store.GiveUpBeforeDispatch(ctx, ref1, domain.GiveUpReranksExhausted); !errors.Is(err, domain.ErrInvalidReference) {
			t.Fatalf("give up with stale generation: got %v, want %v", err, domain.ErrInvalidReference)
		}
		assertReservation(t, readReservation(t, pool, ref2.ID()), "reserved", 2, "pod-b", ref2)
		assertCapacity(t, pool, backendB, backendCapacity{reserved: 2})
		if err := store.GiveUpBeforeDispatch(ctx, ref2, domain.GiveUpReranksExhausted); err != nil {
			t.Fatalf("give up latest generation: %v", err)
		}
		assertReservation(t, readReservation(t, pool, ref2.ID()), "given_up", 2, "pod-b", ref2)
		assertCapacity(t, pool, backendB, backendCapacity{})
		if err := store.GiveUpBeforeDispatch(ctx, ref2, domain.GiveUpReranksExhausted); err != nil {
			t.Fatalf("repeat latest give-up: %v", err)
		}
		assertCapacity(t, pool, backendB, backendCapacity{})
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
				assertCapacity(t, pool, backend, backendCapacity{})
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
		assertCapacity(t, pool, backendA, backendCapacity{})
		assertCapacity(t, pool, backendB, backendCapacity{})
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
		assertCapacity(t, pool, backend, backendCapacity{reserved: 2})
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
		assertCapacity(t, pool, reservedBackend, backendCapacity{})
		assertCapacity(t, pool, abandonedBackend, backendCapacity{})
		assertCapacity(t, pool, authorizedBackend, backendCapacity{orphaned: 2})
		assertActiveDebt(t, pool, authorized.Ref().ID(), authorizedBackend, "tenant-a", "classification_timeout", 2)
		if got := capacityDebtCount(t, pool); got != 1 {
			t.Fatalf("classified debt count = %d, want 1", got)
		}

		classified, err = store.ClassifyExpired(ctx, 10)
		if err != nil {
			t.Fatalf("repeat expired classification: %v", err)
		}
		if classified != 0 {
			t.Fatalf("repeat classified reservations: got %d, want 0", classified)
		}
		assertCapacity(t, pool, reservedBackend, backendCapacity{})
		assertCapacity(t, pool, abandonedBackend, backendCapacity{})
		assertCapacity(t, pool, authorizedBackend, backendCapacity{orphaned: 2})
		if got := capacityDebtCount(t, pool); got != 1 {
			t.Fatalf("repeat classification debt count = %d, want 1", got)
		}
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
		losers := 0
		for call, err := range results {
			if err == nil {
				successes++
				continue
			}
			losers++
			if !errors.Is(err, domain.ErrNoCapacity) {
				t.Fatalf("tenant grant contender %d: got %v, want %v", call, err, domain.ErrNoCapacity)
			}
		}
		if successes != 1 || losers != 1 {
			t.Fatalf("tenant grant contenders: got successes=%d losers=%d, want 1 each; errors=%v", successes, losers, results)
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
		losers := 0
		for call, err := range results {
			if err == nil {
				successes++
				continue
			}
			losers++
			if !errors.Is(err, domain.ErrNoCapacity) {
				t.Fatalf("backend contender %d: got %v, want %v", call, err, domain.ErrNoCapacity)
			}
		}
		if successes != 1 || losers != 1 {
			t.Fatalf("backend contenders: got successes=%d losers=%d, want 1 each; errors=%v", successes, losers, results)
		}
		assertCapacity(t, pool, backend, backendCapacity{reserved: 2})
		if count := admissionGrantCount(t, pool); count != 1 {
			t.Fatalf("admission grant count: got %d, want 1", count)
		}
		tenants := []string{"tenant-a", "tenant-b"}
		for contender, tenant := range tenants {
			wantActive := 0
			if results[contender] == nil {
				wantActive = 2
			}
			assertTenantUsage(t, pool, tenant, wantActive, 0)
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
		assertCapacity(t, pool, backend, backendCapacity{})
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
		assertCapacity(t, pool, backend, backendCapacity{orphaned: 2})
		assertTenantUsage(t, pool, "tenant-a", 0, 2)
		assertGrantState(t, pool, ref.ID(), "orphaned")
		assertActiveDebt(t, pool, ref.ID(), backend, "tenant-a", "ambiguous_transport", 2)
		if got := capacityDebtCount(t, pool); got != 1 {
			t.Fatalf("ambiguous debt count = %d, want 1", got)
		}
	})

	t.Run("identity-gone resolution is exact and idempotent", func(t *testing.T) {
		resetSchedulerLedger(t, pool)
		backend := testIdentity(t, "pod-debt-resolution", 7)
		insertBackend(t, pool, backend)
		command := testIdempotentScheduleCommand(t, "request-debt-resolution", "attempt-debt-resolution", "tenant-a")
		reservation, err := store.TryReserve(ctx, command, backend)
		if err != nil {
			t.Fatalf("reserve debt resolution fixture: %v", err)
		}
		ref := reservation.Ref()
		if _, err := store.PrepareDispatch(ctx, ref); err != nil {
			t.Fatalf("prepare debt resolution fixture: %v", err)
		}
		if err := store.MarkAmbiguous(ctx, ref, domain.AmbiguousProtocol); err != nil {
			t.Fatalf("orphan debt resolution fixture: %v", err)
		}
		replay := testIdempotentScheduleCommand(t, command.RequestID(), "attempt-debt-replay", "tenant-a")
		if _, _, err := store.LookupReservation(ctx, replay); !errors.Is(err, domain.ErrRequestInProgress) {
			t.Fatalf("active debt replay error = %v, want ErrRequestInProgress", err)
		}
		catalog, err := postgresadapter.NewControllerCatalog(pool)
		if err != nil {
			t.Fatalf("create controller catalog: %v", err)
		}
		generation, err := catalog.AcquireControllerWriterGeneration(ctx, backend.Cluster(), "resolver-a")
		if err != nil {
			t.Fatalf("acquire resolver generation: %v", err)
		}
		resolution := testDebtResolution(t, ref.ID(), backend, generation, "proof-a")
		if err := catalog.ResolveCapacityDebt(ctx, resolution); err != nil {
			t.Fatalf("resolve capacity debt: %v", err)
		}
		if err := catalog.ResolveCapacityDebt(ctx, resolution); err != nil {
			t.Fatalf("repeat capacity debt resolution: %v", err)
		}
		assertResolvedDebt(t, pool, ref.ID(), resolution.Proof().EvidenceHash())
		assertCapacity(t, pool, backend, backendCapacity{})
		assertTenantUsage(t, pool, "tenant-a", 0, 0)
		assertGrantState(t, pool, ref.ID(), "released")
		assertReservation(t, readReservation(t, pool, ref.ID()), "orphaned", 1, backend.PodUID(), ref)
		if _, _, err := store.LookupReservation(ctx, replay); !errors.Is(err, domain.ErrRequestNotReplayable) {
			t.Fatalf("resolved debt replay error = %v, want ErrRequestNotReplayable", err)
		}

		conflict := testDebtResolution(t, ref.ID(), backend, generation, "proof-b")
		if err := catalog.ResolveCapacityDebt(ctx, conflict); !errors.Is(err, domain.ErrCapacityDebtConflict) {
			t.Fatalf("conflicting proof error = %v, want ErrCapacityDebtConflict", err)
		}
		missing := testDebtResolutionFor(t, "debt_missing", ref.ID(), backend, generation, "proof-a")
		if err := catalog.ResolveCapacityDebt(ctx, missing); !errors.Is(err, domain.ErrCapacityDebtNotFound) {
			t.Fatalf("missing debt error = %v, want ErrCapacityDebtNotFound", err)
		}
		wrongReservation := testDebtResolutionFor(t, resolution.DebtID(), "wrong-reservation", backend, generation, "proof-a")
		if err := catalog.ResolveCapacityDebt(ctx, wrongReservation); !errors.Is(err, domain.ErrInvalidReference) {
			t.Fatalf("wrong reservation error = %v, want ErrInvalidReference", err)
		}
		for name, wrongIdentity := range map[string]domain.WorkloadIdentity{
			"namespace":      alteredIdentity(t, backend, "wrong", backend.PodUID(), backend.EndpointEpoch(), backend.RecoveryEpoch()),
			"pod uid":        alteredIdentity(t, backend, backend.Namespace(), "wrong-pod", backend.EndpointEpoch(), backend.RecoveryEpoch()),
			"endpoint epoch": alteredIdentity(t, backend, backend.Namespace(), backend.PodUID(), backend.EndpointEpoch()+1, backend.RecoveryEpoch()),
			"recovery epoch": alteredIdentity(t, backend, backend.Namespace(), backend.PodUID(), backend.EndpointEpoch(), backend.RecoveryEpoch()+1),
		} {
			t.Run("reject wrong "+name, func(t *testing.T) {
				wrong := testDebtResolutionFor(t, resolution.DebtID(), ref.ID(), wrongIdentity, generation, "proof-a")
				if err := catalog.ResolveCapacityDebt(ctx, wrong); !errors.Is(err, domain.ErrInvalidReference) {
					t.Fatalf("wrong identity error = %v, want ErrInvalidReference", err)
				}
			})
		}
		newGeneration, err := catalog.AcquireControllerWriterGeneration(ctx, backend.Cluster(), "resolver-b")
		if err != nil {
			t.Fatalf("advance resolver generation: %v", err)
		}
		if newGeneration == generation {
			t.Fatal("writer generation did not advance")
		}
		if err := catalog.ResolveCapacityDebt(ctx, resolution); !errors.Is(err, domain.ErrStaleWriterGeneration) {
			t.Fatalf("stale duplicate error = %v, want ErrStaleWriterGeneration", err)
		}
		assertResolvedDebt(t, pool, ref.ID(), resolution.Proof().EvidenceHash())
		assertCapacity(t, pool, backend, backendCapacity{})
		assertTenantUsage(t, pool, "tenant-a", 0, 0)
	})

	t.Run("concurrent identical identity-gone proofs release once", func(t *testing.T) {
		resetSchedulerLedger(t, pool)
		backend := testIdentity(t, "pod-debt-concurrent", 8)
		insertBackend(t, pool, backend)
		reservation, err := store.TryReserve(ctx,
			testScheduleCommand(t, "request-debt-concurrent", "attempt-debt-concurrent", "tenant-a"), backend)
		if err != nil {
			t.Fatalf("reserve concurrent debt fixture: %v", err)
		}
		ref := reservation.Ref()
		if _, err := store.PrepareDispatch(ctx, ref); err != nil {
			t.Fatalf("prepare concurrent debt fixture: %v", err)
		}
		if err := store.MarkAmbiguous(ctx, ref, domain.AmbiguousCanceled); err != nil {
			t.Fatalf("orphan concurrent debt fixture: %v", err)
		}
		catalog, err := postgresadapter.NewControllerCatalog(pool)
		if err != nil {
			t.Fatalf("create controller catalog: %v", err)
		}
		generation, err := catalog.AcquireControllerWriterGeneration(ctx, backend.Cluster(), "resolver-concurrent")
		if err != nil {
			t.Fatalf("acquire concurrent resolver generation: %v", err)
		}
		resolution := testDebtResolution(t, ref.ID(), backend, generation, "proof-concurrent")
		results := make([]error, 2)
		start := make(chan struct{})
		var calls sync.WaitGroup
		for call := range results {
			calls.Add(1)
			go func(call int) {
				defer calls.Done()
				<-start
				results[call] = catalog.ResolveCapacityDebt(ctx, resolution)
			}(call)
		}
		close(start)
		calls.Wait()
		for call, err := range results {
			if err != nil {
				t.Fatalf("concurrent resolve call %d: %v", call, err)
			}
		}
		assertResolvedDebt(t, pool, ref.ID(), resolution.Proof().EvidenceHash())
		assertCapacity(t, pool, backend, backendCapacity{})
		assertTenantUsage(t, pool, "tenant-a", 0, 0)
		assertGrantState(t, pool, ref.ID(), "released")
	})

	t.Run("resolver rejects inconsistent non-orphaned reservation atomically", func(t *testing.T) {
		resetSchedulerLedger(t, pool)
		backend := testIdentity(t, "pod-debt-inconsistent", 9)
		insertBackend(t, pool, backend)
		reservation, err := store.TryReserve(ctx,
			testScheduleCommand(t, "request-debt-inconsistent", "attempt-debt-inconsistent", "tenant-a"), backend)
		if err != nil {
			t.Fatalf("reserve inconsistent debt fixture: %v", err)
		}
		ref := reservation.Ref()
		if _, err := store.PrepareDispatch(ctx, ref); err != nil {
			t.Fatalf("prepare inconsistent debt fixture: %v", err)
		}
		if err := store.MarkAmbiguous(ctx, ref, domain.AmbiguousTransport); err != nil {
			t.Fatalf("orphan inconsistent debt fixture: %v", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE scheduler_reservations SET state='dispatch_authorized' WHERE reservation_id=$1`, ref.ID()); err != nil {
			t.Fatalf("corrupt reservation state fixture: %v", err)
		}
		catalog, err := postgresadapter.NewControllerCatalog(pool)
		if err != nil {
			t.Fatalf("create controller catalog: %v", err)
		}
		generation, err := catalog.AcquireControllerWriterGeneration(ctx, backend.Cluster(), "resolver-inconsistent")
		if err != nil {
			t.Fatalf("acquire inconsistent resolver generation: %v", err)
		}
		resolution := testDebtResolution(t, ref.ID(), backend, generation, "proof-inconsistent")
		if err := catalog.ResolveCapacityDebt(ctx, resolution); !errors.Is(err, domain.ErrInvalidState) {
			t.Fatalf("inconsistent reservation error = %v, want ErrInvalidState", err)
		}
		assertActiveDebt(t, pool, ref.ID(), backend, "tenant-a", "ambiguous_transport", 2)
		assertCapacity(t, pool, backend, backendCapacity{orphaned: 2})
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
		assertCapacity(t, pool, backend, backendCapacity{})
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
			assertEmptyContributions(t, pool, backend, "tenant-denied")
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
			assertEmptyContributions(t, pool, backend, "tenant-a")
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
				assertEmptyContributions(t, pool, backend, "tenant-a")
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
	command, err := domain.NewScheduleCommand(domain.ScheduleParams{
		RequestID: requestID, AttemptID: attemptID, Tenant: tenant,
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
