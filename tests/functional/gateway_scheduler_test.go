package functional_test

import (
	"context"
	"crypto/sha256"
	"errors"

	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	schedulerv1 "github.com/BizerNotNull/474-Prudentia/api/scheduler/v1"
	"github.com/BizerNotNull/474-Prudentia/internal/adapter/schedulerclient"
	"github.com/BizerNotNull/474-Prudentia/internal/auth"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	requestapp "github.com/BizerNotNull/474-Prudentia/internal/request"
	"github.com/BizerNotNull/474-Prudentia/internal/transport/publichttp"
	transport "github.com/BizerNotNull/474-Prudentia/internal/transport/schedulergrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type ledgerScheduler struct {
	mu           sync.Mutex
	state        string
	ref          domain.ReservationRef
	target       domain.DispatchTarget
	hasKey       bool
	lookup       [32]byte
	digest       [32]byte
	staleOnce    bool
	prepareCalls int
}

func newLedgerScheduler(t *testing.T) *ledgerScheduler {
	t.Helper()
	ref, err := domain.NewReservationRef("res_test", 1, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: "cluster-a", Namespace: "inference", LogicalEngine: "engine-a", PodUID: "pod-uid-a", EndpointEpoch: 1, RecoveryEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	target, err := domain.NewDispatchTarget("https://provider.invalid", identity)
	if err != nil {
		t.Fatal(err)
	}
	return &ledgerScheduler{ref: ref, target: target}
}

func (s *ledgerScheduler) Schedule(_ context.Context, command domain.ScheduleCommand) (domain.Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == "abandoned_rerank" {
		ref, err := domain.NewReservationRef(s.ref.ID(), s.ref.Generation()+1, s.ref.Capability())
		if err != nil {
			return domain.Reservation{}, err
		}
		s.ref = ref
		s.state = "reserved"
		return domain.NewReservation(ref), nil
	}

	lookups, digests := command.IdempotencyCandidates(), command.DigestCandidates()
	if s.state != "" {
		if !s.hasKey || len(lookups) != 1 || len(digests) != 1 || lookups[0].Value() != s.lookup {
			return domain.Reservation{}, domain.ErrInvalidState
		}
		if digests[0].Value() != s.digest {
			return domain.Reservation{}, domain.ErrIdempotencyConflict
		}
		if s.state == "reserved" || s.state == "dispatch_authorized" || s.state == "orphaned" {
			return domain.Reservation{}, domain.ErrRequestInProgress
		}
		return domain.Reservation{}, domain.ErrRequestNotReplayable
	}
	if len(lookups) == 1 && len(digests) == 1 {
		s.hasKey = true
		s.lookup, s.digest = lookups[0].Value(), digests[0].Value()
	}
	s.state = "reserved"
	return domain.NewReservation(s.ref), nil
}
func (s *ledgerScheduler) PrepareDispatch(context.Context, domain.ReservationRef) (domain.DispatchTarget, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prepareCalls++
	if s.staleOnce && s.prepareCalls == 1 {
		return domain.DispatchTarget{}, domain.ErrStaleTarget
	}

	if s.state != "reserved" {
		return domain.DispatchTarget{}, domain.ErrInvalidState
	}
	s.state = "dispatch_authorized"
	return s.target, nil
}
func (s *ledgerScheduler) AbandonBeforeDispatch(context.Context, domain.ReservationRef, domain.RerankReason) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != "reserved" {
		return domain.ErrInvalidState
	}
	s.state = "abandoned_rerank"
	return nil
}

func (s *ledgerScheduler) GiveUpBeforeDispatch(context.Context, domain.ReservationRef, domain.GiveUpReason) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = "given_up"
	return nil
}
func (s *ledgerScheduler) Finalize(context.Context, domain.ReservationRef, domain.TerminalProof) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != "dispatch_authorized" {
		return domain.ErrInvalidState
	}
	s.state = "released"
	return nil
}
func (s *ledgerScheduler) MarkAmbiguous(context.Context, domain.ReservationRef, domain.AmbiguousCause) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = "orphaned"
	return nil
}

type successfulProvider struct {
	mu    sync.Mutex
	calls int
}

func (p *successfulProvider) Infer(ctx context.Context, _ domain.DispatchTarget, _ domain.AuthorizedRequest, sink publichttp.StreamSink) error {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	delta, _ := domain.NewDeltaEvent("scheduled response")
	usage, _ := domain.NewUsageEvent(domain.Usage{PromptTokens: 2, CompletionTokens: 2, TotalTokens: 4})
	for _, event := range []domain.StreamEvent{delta, usage, domain.NewTerminalEvent()} {
		if err := sink.Write(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func TestGatewayCompletesRequestThroughSchedulerGRPC(t *testing.T) {
	ledger := newLedgerScheduler(t)
	serverBoundary, err := transport.NewServer(ledger)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	schedulerv1.RegisterSchedulerServiceServer(grpcServer, serverBoundary)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	connection, err := grpc.NewClient("passthrough:///scheduler", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client, err := schedulerclient.New(schedulerv1.NewSchedulerServiceClient(connection))
	if err != nil {
		t.Fatal(err)
	}
	provider := &successfulProvider{}
	inference, err := requestapp.NewService(client, provider, requestapp.IdempotencyConfig{
		LookupKeys: []requestapp.VersionedKey{{Version: 1, Key: []byte("lookup-key-32-bytes-long-value!!")}}, LookupWriteVersion: 1,
		DigestKeys: []requestapp.VersionedKey{{Version: 1, Key: []byte("digest-key-32-bytes-long-value!!")}}, DigestWriteVersion: 1,
	}, time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := auth.NewAuthenticator([]auth.APIKey{{Token: "0123456789abcdef0123456789abcdef", Tenant: "tenant-a", Models: []string{"model-a"}}})
	if err != nil {
		t.Fatal(err)
	}
	handler := publichttp.NewHandler(authenticator, auth.Authorizer{}, inference, publichttp.DefaultLimits()).Routes(http.NotFoundHandler())

	makeRequest := func(body string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer 0123456789abcdef0123456789abcdef")
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "client-operation-1")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	response := makeRequest(`{"model":"model-a","messages":[{"role":"user","content":"hello"}]}`)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var payload struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Choices) != 1 || payload.Choices[0].Message.Content != "scheduled response" {
		t.Fatalf("unexpected response: %s", response.Body.String())
	}
	replay := makeRequest(`{"model":"model-a","messages":[{"role":"user","content":"hello"}]}`)
	if replay.Code != http.StatusConflict || !strings.Contains(replay.Body.String(), `"request_not_replayable"`) {
		t.Fatalf("replay status = %d, body = %s", replay.Code, replay.Body.String())
	}
	conflict := makeRequest(`{"model":"model-a","messages":[{"role":"user","content":"different"}]}`)
	if conflict.Code != http.StatusConflict || !strings.Contains(conflict.Body.String(), `"idempotency_conflict"`) {
		t.Fatalf("conflict status = %d, body = %s", conflict.Code, conflict.Body.String())
	}
	provider.mu.Lock()
	providerCalls := provider.calls
	provider.mu.Unlock()
	if providerCalls != 1 {
		t.Fatalf("provider calls = %d, want 1", providerCalls)
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.state != "released" {
		t.Fatalf("ledger state = %q, want released", ledger.state)
	}
}

func TestCandidateSwitchRoundTripsThroughSchedulerGRPC(t *testing.T) {
	ledger := newLedgerScheduler(t)
	ledger.staleOnce = true
	serverBoundary, err := transport.NewServer(ledger)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	schedulerv1.RegisterSchedulerServiceServer(grpcServer, serverBoundary)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	connection, err := grpc.NewClient("passthrough:///scheduler", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client, err := schedulerclient.New(schedulerv1.NewSchedulerServiceClient(connection))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("candidate-switch-command"))
	digestCandidate, err := domain.NewRequestDigestCandidate(1, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	command, err := domain.NewScheduleCommand(domain.ScheduleParams{
		RequestID: "req_switch", AttemptID: "att_switch", Tenant: "tenant-a",
		DigestCandidates: []domain.RequestDigestCandidate{digestCandidate}, DigestWriteVersion: 1,
		Model: "model-a", SlotCost: 1, Features: domain.EmptyFeatureSet(), ExecutionBudget: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := client.Schedule(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PrepareDispatch(context.Background(), first.Ref()); !errors.Is(err, domain.ErrStaleTarget) {
		t.Fatalf("prepare error = %v, want stale target", err)
	}
	if err := client.AbandonBeforeDispatch(context.Background(), first.Ref(), domain.RerankStaleTarget); err != nil {
		t.Fatalf("abandon candidate: %v", err)
	}
	second, err := client.Schedule(context.Background(), command)
	if err != nil {
		t.Fatalf("schedule replacement candidate: %v", err)
	}
	if second.Ref().Generation() != first.Ref().Generation()+1 {
		t.Fatalf("replacement generation = %d, first = %d", second.Ref().Generation(), first.Ref().Generation())
	}
	if _, err := client.PrepareDispatch(context.Background(), second.Ref()); err != nil {
		t.Fatalf("prepare replacement candidate: %v", err)
	}
}
