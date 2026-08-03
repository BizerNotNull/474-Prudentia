package functional_test

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	adminv1 "github.com/BizerNotNull/474-Prudentia/api/admin/v1"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/BizerNotNull/474-Prudentia/internal/transport/admingrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type adminResolver struct {
	identity domain.WorkloadIdentity
	calls    int
}

func (r *adminResolver) ResolveDebtIdentity(context.Context, string, string, uint64) (domain.WorkloadIdentity, error) {
	r.calls++
	return r.identity, nil
}

type adminPolicy struct {
	allow bool
	calls int
}

func (p *adminPolicy) Allows(context.Context, domain.AdminPrincipal, domain.AdminAction, admingrpc.DebtTarget) bool {
	p.calls++
	return p.allow
}

type auditedOverride struct {
	calls int
	audit []domain.UnsafeDebtOverrideAuditEvent
	fail  error
}

func (s *auditedOverride) UnsafeOverrideCapacityDebt(_ context.Context, command domain.UnsafeDebtOverride) error {
	if s.fail != nil {
		return s.fail
	}
	event, err := domain.NewUnsafeDebtOverrideAuditEvent(command, time.Unix(3000, 0).UTC())
	if err != nil {
		return err
	}
	s.calls++
	s.audit = append(s.audit, event)
	return nil
}
func adminTLSContext(t *testing.T, rawURI string) context.Context {
	t.Helper()
	uri, err := url.Parse(rawURI)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Minute), URIs: []*url.URL{uri}}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}}}})
}
func adminOverrideRequest() *adminv1.UnsafeOverrideCapacityDebtRequest {
	return &adminv1.UnsafeOverrideCapacityDebtRequest{DebtId: "debt-1", ExpectedPodUid: "pod-1", ExpectedEndpointEpoch: 7, Confirmation: admingrpc.ConfirmationPhrase, Ticket: "OPS-123", Reason: "manual incident response", SchemaVersion: 1}
}

func TestAdminAuthzRedactionAndAuditInterfaces(t *testing.T) {
	identity, _ := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: "cluster-1", Namespace: "models", LogicalEngine: "engine", PodUID: "pod-1", EndpointEpoch: 7, RecoveryEpoch: 2})
	resolver := &adminResolver{identity: identity}
	policy := &adminPolicy{}
	service := &auditedOverride{}
	server, err := admingrpc.NewAdminServer(&admingrpc.AdminAuthenticator{TrustDomain: "test.local", AllowedPathPrefixes: []string{"/operator/"}}, &admingrpc.AdminAuthorizer{Policy: policy}, admingrpc.AdminCodec{Resolver: resolver}, service)
	if err != nil {
		t.Fatal(err)
	}
	ctx := adminTLSContext(t, "spiffe://test.local/operator/alice")
	if _, err := server.UnsafeOverrideCapacityDebt(ctx, adminOverrideRequest()); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("denied code = %v", status.Code(err))
	}
	if resolver.calls != 0 || service.calls != 0 {
		t.Fatal("denied request reached debt lookup or mutation")
	}
	policy.allow = true
	if _, err := server.UnsafeOverrideCapacityDebt(ctx, adminOverrideRequest()); err != nil {
		t.Fatal(err)
	}
	if service.calls != 1 || len(service.audit) != 1 {
		t.Fatalf("mutation/audit count = %d/%d", service.calls, len(service.audit))
	}
	event := service.audit[0]
	if event.Type() != domain.DebtAuditEventCapacityDebtUnsafeOverridden || event.EventHash() == ([sha256.Size]byte{}) {
		t.Fatal("audit event lacks immutable identity")
	}
	if strings.Contains(event.String(), "OPS-123") || strings.Contains(event.String(), "manual incident") {
		t.Fatal("audit String leaked operator input")
	}
	bad := adminOverrideRequest()
	bad.Confirmation = "true"
	if _, err := server.UnsafeOverrideCapacityDebt(ctx, bad); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("danger phrase code = %v", status.Code(err))
	}
	if service.calls != 1 {
		t.Fatal("invalid danger phrase reached mutation")
	}
	service.fail = errors.New("database failure includes OPS-123 manual incident response")
	_, err = server.UnsafeOverrideCapacityDebt(ctx, adminOverrideRequest())
	if status.Code(err) != codes.Internal || strings.Contains(err.Error(), "OPS-123") || strings.Contains(err.Error(), "manual incident") {
		t.Fatalf("unsafe error disclosure: %v", err)
	}
}

func TestAdminAuthenticationRejectsRequestPlaneIdentity(t *testing.T) {
	authenticator := &admingrpc.AdminAuthenticator{TrustDomain: "test.local", AllowedPathPrefixes: []string{"/operator/"}}
	if _, err := authenticator.Authenticate(adminTLSContext(t, "spiffe://test.local/ns/system/sa/gateway")); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v", status.Code(err))
	}
}
