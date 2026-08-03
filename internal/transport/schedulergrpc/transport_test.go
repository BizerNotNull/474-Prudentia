package schedulergrpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	schedulerv1 "github.com/BizerNotNull/474-Prudentia/api/scheduler/v1"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const testGatewayID = "spiffe://test.local/ns/gateway/sa/gateway"

type schedulerStub struct{ calls int }

func (s *schedulerStub) Schedule(context.Context, domain.ScheduleCommand) (domain.Reservation, error) {
	s.calls++
	ref, _ := domain.NewReservationRef("reservation-1", 1, []byte("0123456789abcdef"))
	return domain.NewReservation(ref), nil
}
func (*schedulerStub) PrepareDispatch(context.Context, domain.ReservationRef) (domain.DispatchTarget, error) {
	id, _ := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: "c", Namespace: "n", LogicalEngine: "e", PodUID: "p", EndpointEpoch: 1, RecoveryEpoch: 1})
	return domain.NewDispatchTarget("https://provider.local", id)
}
func (*schedulerStub) AbandonBeforeDispatch(context.Context, domain.ReservationRef, domain.RerankReason) error {
	return nil
}
func (*schedulerStub) GiveUpBeforeDispatch(context.Context, domain.ReservationRef, domain.GiveUpReason) error {
	return nil
}
func (*schedulerStub) Finalize(context.Context, domain.ReservationRef, domain.TerminalProof) error {
	return nil
}
func (*schedulerStub) MarkAmbiguous(context.Context, domain.ReservationRef, domain.AmbiguousCause) error {
	return nil
}

func validScheduleRequest() *schedulerv1.ScheduleRequest {
	digest := make([]byte, 32)
	return &schedulerv1.ScheduleRequest{RequestId: "request-1", AttemptId: "attempt-1", TenantScope: []byte("tenant"), DigestCandidates: []*schedulerv1.RequestDigestCandidate{{DigestVersion: 1, HmacSha256: digest}}, DigestWriteVersion: 1, Model: "model", SlotCost: 1, ExecutionBudgetMs: 1000, Features: &schedulerv1.FeatureSet{SchemaVersion: 1}, Priority: schedulerv1.Priority_PRIORITY_NORMAL, SchemaVersion: 1, BudgetSchemaVersion: 1}
}

func TestCodecFailsClosedAndRedactsErrors(t *testing.T) {
	codec := Codec{}
	request := validScheduleRequest()
	request.Priority = schedulerv1.Priority(99)
	if _, err := codec.DecodeSchedule(request); err == nil {
		t.Fatal("unknown priority accepted")
	}
	request = validScheduleRequest()
	request.SchemaVersion = 2
	if _, err := codec.DecodeSchedule(request); err == nil {
		t.Fatal("unknown schema accepted")
	}
	secret := "raw-capability-and-prompt"
	err := transportError(codec, errorsNew(secret))
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("transport error leaked secret: %v", err)
	}
	st, _ := status.FromError(err)
	if len(st.Details()) != 1 {
		t.Fatalf("typed details = %d", len(st.Details()))
	}
	detail, ok := st.Details()[0].(*schedulerv1.ErrorDetail)
	if !ok || detail.SchemaVersion != 1 || detail.Code != schedulerv1.ErrorCode_ERROR_CODE_INTERNAL {
		t.Fatalf("unexpected detail: %#v", st.Details())
	}
}

type secretError string

func (e secretError) Error() string { return string(e) }
func errorsNew(value string) error  { return secretError(value) }

func TestBufconnGatewayIdentityDeadlineAndCodec(t *testing.T) {
	ca, serverCert, gatewayCert := testCertificates(t, testGatewayID)
	stub := &schedulerStub{}
	service, err := NewServer(stub)
	if err != nil {
		t.Fatal(err)
	}
	interceptor, err := NewGatewayUnaryInterceptor(GatewayInterceptorConfig{AllowedSPIFFEIDs: []string{testGatewayID}, MaxDeadline: time.Second, MaxMessageBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	serverTLS := &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientCAs: ca, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13}
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)), grpc.UnaryInterceptor(interceptor))
	schedulerv1.RegisterSchedulerServiceServer(grpcServer, service)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	clientTLS := &tls.Config{Certificates: []tls.Certificate{gatewayCert}, RootCAs: ca, ServerName: "scheduler.test", MinVersion: tls.VersionTLS13}
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := schedulerv1.NewSchedulerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	response, err := client.Schedule(ctx, validScheduleRequest())
	if err != nil {
		t.Fatal(err)
	}
	if response.GetReservation().GetRef().GetSchemaVersion() != 1 || stub.calls != 1 {
		t.Fatalf("response=%v calls=%d", response, stub.calls)
	}
	oversized := validScheduleRequest()
	oversized.Model = strings.Repeat("x", 5000)
	oversizedCtx, oversizedCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer oversizedCancel()
	if _, err := client.Schedule(oversizedCtx, oversized); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized message code = %v", status.Code(err))
	}
	_, err = client.Schedule(context.Background(), validScheduleRequest())
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing deadline code = %v", status.Code(err))
	}
}

func testCertificates(t *testing.T, clientURI string) (*x509.CertPool, tls.Certificate, tls.Certificate) {
	t.Helper()
	now := time.Now()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	issue := func(serial int64, server bool, uri string) tls.Certificate {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
		if server {
			tmpl.Subject = pkix.Name{CommonName: "scheduler.test"}
			tmpl.DNSNames = []string{"scheduler.test"}
			tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		} else {
			parsed, parseErr := url.Parse(uri)
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			tmpl.URIs = []*url.URL{parsed}
			tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		}
		der, createErr := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if createErr != nil {
			t.Fatal(createErr)
		}
		return tls.Certificate{Certificate: [][]byte{der, caDER}, PrivateKey: key}
	}
	return pool, issue(2, true, ""), issue(3, false, clientURI)
}
