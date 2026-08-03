package admingrpc

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"testing"
	"time"

	adminv1 "github.com/BizerNotNull/474-Prudentia/api/admin/v1"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"strings"
)

type identityResolver struct {
	identity domain.WorkloadIdentity
	calls    int
}

func (r *identityResolver) ResolveDebtIdentity(context.Context, string, string, uint64) (domain.WorkloadIdentity, error) {
	r.calls++
	return r.identity, nil
}

type permissionPolicy struct {
	allow bool
	calls int
}

func (p *permissionPolicy) Allows(context.Context, domain.AdminPrincipal, domain.AdminAction, DebtTarget) bool {
	p.calls++
	return p.allow
}

type overrideService struct {
	calls   int
	command domain.UnsafeDebtOverride
	err     error
}

func (s *overrideService) UnsafeOverrideCapacityDebt(_ context.Context, command domain.UnsafeDebtOverride) error {
	s.calls++
	s.command = command
	return s.err
}

func adminContext(t *testing.T, rawURI string) context.Context {
	t.Helper()
	uri, err := url.Parse(rawURI)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Minute), URIs: []*url.URL{uri}}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}}}})
}

func request() *adminv1.UnsafeOverrideCapacityDebtRequest {
	return &adminv1.UnsafeOverrideCapacityDebtRequest{DebtId: "debt-1", ExpectedPodUid: "pod-1", ExpectedEndpointEpoch: 7, Confirmation: ConfirmationPhrase, Ticket: "OPS-123", Reason: "manual incident response", SchemaVersion: 1}
}

func TestAdminAuthenticatorDeniesGatewayAndRevokedIdentity(t *testing.T) {
	auth := &AdminAuthenticator{TrustDomain: "test.local", AllowedPathPrefixes: []string{"/operator/", "/automation/break-glass/"}}
	if _, err := auth.Authenticate(adminContext(t, "spiffe://test.local/ns/system/sa/gateway")); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("gateway code = %v", status.Code(err))
	}
	ctx := adminContext(t, "spiffe://test.local/operator/alice")
	principal, err := auth.Authenticate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	expected := sha256.Sum256([]byte("spiffe://test.local/operator/alice"))
	if principal.IdentityHash() != expected {
		t.Fatal("principal was not bound to authenticated URI")
	}
	auth.RevokedIdentityHashes = map[[sha256.Size]byte]struct{}{expected: {}}
	if _, err := auth.Authenticate(ctx); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("revoked code = %v", status.Code(err))
	}
}

func TestAdminServerAuthorizesBeforeDecodeAndMutation(t *testing.T) {
	identity, _ := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: "cluster-1", Namespace: "ns", LogicalEngine: "engine", PodUID: "pod-1", EndpointEpoch: 7, RecoveryEpoch: 2})
	resolver := &identityResolver{identity: identity}
	policy := &permissionPolicy{}
	service := &overrideService{}
	server, err := NewAdminServer(&AdminAuthenticator{TrustDomain: "test.local", AllowedPathPrefixes: []string{"/operator/"}}, &AdminAuthorizer{Policy: policy}, AdminCodec{Resolver: resolver}, service)
	if err != nil {
		t.Fatal(err)
	}
	ctx := adminContext(t, "spiffe://test.local/operator/alice")
	if _, err := server.UnsafeOverrideCapacityDebt(ctx, request()); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("deny code = %v", status.Code(err))
	}
	if resolver.calls != 0 || service.calls != 0 {
		t.Fatal("denied call reached lookup or mutation")
	}
	policy.allow = true
	if _, err := server.UnsafeOverrideCapacityDebt(ctx, request()); err != nil {
		t.Fatal(err)
	}
	if service.calls != 1 || service.command.Principal().IdentityHash() == ([sha256.Size]byte{}) {
		t.Fatal("authenticated principal was not bound")
	}
	bad := request()
	bad.Confirmation += "!"
	if _, err := server.UnsafeOverrideCapacityDebt(ctx, bad); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad confirmation code = %v", status.Code(err))
	}
	if service.calls != 1 {
		t.Fatal("invalid request reached mutation")
	}
	service.err = errors.New("secret ticket OPS-123 and reason")
	if _, err := server.UnsafeOverrideCapacityDebt(ctx, request()); status.Code(err) != codes.Internal || strings.Contains(err.Error(), "OPS-123") {
		t.Fatalf("admin error was not safely redacted: %v", err)
	}
}
