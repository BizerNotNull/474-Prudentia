package functional_test

import (
	"context"
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
	mu     sync.Mutex
	state  string
	ref    domain.ReservationRef
	target domain.DispatchTarget
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

func (s *ledgerScheduler) Schedule(context.Context, domain.ScheduleCommand) (domain.Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != "" {
		return domain.Reservation{}, domain.ErrInvalidState
	}
	s.state = "reserved"
	return domain.NewReservation(s.ref), nil
}
func (s *ledgerScheduler) PrepareDispatch(context.Context, domain.ReservationRef) (domain.DispatchTarget, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != "reserved" {
		return domain.DispatchTarget{}, domain.ErrInvalidState
	}
	s.state = "dispatch_authorized"
	return s.target, nil
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

type successfulProvider struct{}

func (successfulProvider) Infer(ctx context.Context, _ domain.DispatchTarget, _ domain.AuthorizedRequest, sink publichttp.StreamSink) error {
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
	inference, err := requestapp.NewService(client, successfulProvider{}, time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := auth.NewAuthenticator([]auth.APIKey{{Token: "0123456789abcdef0123456789abcdef", Tenant: "tenant-a", Models: []string{"model-a"}}})
	if err != nil {
		t.Fatal(err)
	}
	handler := publichttp.NewHandler(authenticator, auth.Authorizer{}, inference, publichttp.DefaultLimits()).Routes(http.NotFoundHandler())

	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[{"role":"user","content":"hello"}]}`))
	request.Header.Set("Authorization", "Bearer 0123456789abcdef0123456789abcdef")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
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
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.state != "released" {
		t.Fatalf("ledger state = %q, want released", ledger.state)
	}
}
