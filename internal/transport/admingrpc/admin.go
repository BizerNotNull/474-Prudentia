package admingrpc

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"time"

	adminv1 "github.com/BizerNotNull/474-Prudentia/api/admin/v1"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	SchemaVersionV1    uint32 = 1
	ConfirmationPhrase        = "I_UNDERSTAND_THE_PROVIDER_MAY_STILL_BE_EXECUTING"
	MaxDebtIDBytes            = 256
	MaxTicketBytes            = 256
	MaxReasonBytes            = 1024
)

type AdminAuthenticator struct {
	TrustDomain           string
	AllowedPathPrefixes   []string
	RevokedIdentityHashes map[[sha256.Size]byte]struct{}
	Now                   func() time.Time
}

func (a *AdminAuthenticator) Authenticate(ctx context.Context) (domain.AdminPrincipal, error) {
	if a == nil || a.TrustDomain == "" || len(a.AllowedPathPrefixes) == 0 {
		return domain.AdminPrincipal{}, status.Error(codes.Unauthenticated, "admin identity required")
	}
	p, ok := peer.FromContext(ctx)
	if !ok {
		return domain.AdminPrincipal{}, status.Error(codes.Unauthenticated, "admin identity required")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return domain.AdminPrincipal{}, status.Error(codes.Unauthenticated, "admin identity required")
	}
	cert := tlsInfo.State.VerifiedChains[0][0]
	now := time.Now()
	if a.Now != nil {
		now = a.Now()
	}
	if cert == nil || now.Before(cert.NotBefore) || now.After(cert.NotAfter) || len(cert.URIs) != 1 {
		return domain.AdminPrincipal{}, status.Error(codes.Unauthenticated, "admin identity required")
	}
	u := cert.URIs[0]
	if u.Scheme != "spiffe" || u.Host != a.TrustDomain || u.User != nil || u.RawQuery != "" || u.Fragment != "" || isServiceIdentity(u.Path) {
		return domain.AdminPrincipal{}, status.Error(codes.PermissionDenied, "admin identity denied")
	}
	allowed := false
	for _, prefix := range a.AllowedPathPrefixes {
		if strings.HasPrefix(u.Path, prefix) {
			allowed = true
			break
		}
	}
	if !allowed {
		return domain.AdminPrincipal{}, status.Error(codes.PermissionDenied, "admin identity denied")
	}
	hash := sha256.Sum256([]byte(u.String()))
	if _, revoked := a.RevokedIdentityHashes[hash]; revoked {
		return domain.AdminPrincipal{}, status.Error(codes.PermissionDenied, "admin identity denied")
	}
	principal, err := domain.NewAdminPrincipal(hash)
	if err != nil {
		return domain.AdminPrincipal{}, status.Error(codes.Unauthenticated, "admin identity required")
	}
	return principal, nil
}

func isServiceIdentity(path string) bool {
	return strings.Contains(path, "/gateway") || strings.Contains(path, "/controller") || strings.HasPrefix(path, "/ns/")
}

type DebtTarget struct {
	DebtID, PodUID string
	EndpointEpoch  uint64
	Identity       domain.WorkloadIdentity
}

type PermissionPolicy interface {
	Allows(context.Context, domain.AdminPrincipal, domain.AdminAction, DebtTarget) bool
}

type AdminAuthorizer struct{ Policy PermissionPolicy }

func (a *AdminAuthorizer) AuthorizeUnsafeDebtOverride(ctx context.Context, p domain.AdminPrincipal, target DebtTarget) error {
	if a == nil || a.Policy == nil || target.DebtID == "" || !a.Policy.Allows(ctx, p, domain.AdminActionCapacityDebtUnsafeOverride, target) {
		return status.Error(codes.PermissionDenied, "unsafe override denied")
	}
	return nil
}

type IdentityResolver interface {
	ResolveDebtIdentity(context.Context, string, string, uint64) (domain.WorkloadIdentity, error)
}

type AdminCodec struct{ Resolver IdentityResolver }

func (c AdminCodec) DecodeUnsafeOverride(m *adminv1.UnsafeOverrideCapacityDebtRequest, p domain.AdminPrincipal) (domain.UnsafeDebtOverride, error) {
	if m == nil || m.SchemaVersion != SchemaVersionV1 || len(m.DebtId) == 0 || len(m.DebtId) > MaxDebtIDBytes || len(m.ExpectedPodUid) == 0 || len(m.ExpectedPodUid) > 128 || m.ExpectedEndpointEpoch == 0 || m.Confirmation != ConfirmationPhrase || len(m.Ticket) == 0 || len(m.Ticket) > MaxTicketBytes || len(m.Reason) == 0 || len(m.Reason) > MaxReasonBytes || strings.TrimSpace(m.Ticket) != m.Ticket || strings.TrimSpace(m.Reason) != m.Reason || c.Resolver == nil {
		return domain.UnsafeDebtOverride{}, domain.ErrInvalidUnsafeDebtOverride
	}
	identity, err := c.Resolver.ResolveDebtIdentity(context.Background(), m.DebtId, m.ExpectedPodUid, m.ExpectedEndpointEpoch)
	if err != nil || identity.PodUID() != m.ExpectedPodUid || identity.EndpointEpoch() != m.ExpectedEndpointEpoch {
		return domain.UnsafeDebtOverride{}, domain.ErrInvalidUnsafeDebtOverride
	}
	// The wire phrase is translated only after exact validation; the domain constructor remains the final invariant gate.
	return domain.NewUnsafeDebtOverride(domain.UnsafeDebtOverrideParams{DebtID: m.DebtId, ExpectedIdentity: identity, Principal: p, Confirmation: domain.UnsafeDebtOverrideDangerPhrase, Ticket: m.Ticket, Reason: m.Reason})
}

type UnsafeOverrideService interface {
	UnsafeOverrideCapacityDebt(context.Context, domain.UnsafeDebtOverride) error
}

type AdminServer struct {
	adminv1.UnimplementedCapacityDebtAdminServiceServer
	Authenticator *AdminAuthenticator
	Authorizer    *AdminAuthorizer
	Codec         AdminCodec
	Service       UnsafeOverrideService
}

func NewAdminServer(authenticator *AdminAuthenticator, authorizer *AdminAuthorizer, codec AdminCodec, service UnsafeOverrideService) (*AdminServer, error) {
	if authenticator == nil || authorizer == nil || codec.Resolver == nil || service == nil {
		return nil, errors.New("complete admin transport dependencies are required")
	}
	return &AdminServer{Authenticator: authenticator, Authorizer: authorizer, Codec: codec, Service: service}, nil
}

func (s *AdminServer) UnsafeOverrideCapacityDebt(ctx context.Context, m *adminv1.UnsafeOverrideCapacityDebtRequest) (*emptypb.Empty, error) {
	principal, err := s.Authenticator.Authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if m == nil || m.SchemaVersion != SchemaVersionV1 || s.Codec.Resolver == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid unsafe override")
	}
	target := DebtTarget{DebtID: m.DebtId, PodUID: m.ExpectedPodUid, EndpointEpoch: m.ExpectedEndpointEpoch}
	if err := s.Authorizer.AuthorizeUnsafeDebtOverride(ctx, principal, target); err != nil {
		return nil, err
	}
	identity, err := s.Codec.Resolver.ResolveDebtIdentity(ctx, m.DebtId, m.ExpectedPodUid, m.ExpectedEndpointEpoch)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid unsafe override")
	}
	codec := s.Codec
	codec.Resolver = fixedResolver{identity: identity}
	command, err := codec.DecodeUnsafeOverride(m, principal)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid unsafe override")
	}
	if err := s.Service.UnsafeOverrideCapacityDebt(ctx, command); err != nil {
		return nil, adminError(err)
	}
	return &emptypb.Empty{}, nil
}

type fixedResolver struct{ identity domain.WorkloadIdentity }

func (r fixedResolver) ResolveDebtIdentity(context.Context, string, string, uint64) (domain.WorkloadIdentity, error) {
	return r.identity, nil
}

func adminError(err error) error {
	switch {
	case errors.Is(err, domain.ErrInvalidUnsafeDebtOverride):
		return status.Error(codes.InvalidArgument, "invalid unsafe override")
	case errors.Is(err, domain.ErrInvalidState):
		return status.Error(codes.FailedPrecondition, "debt cannot transition")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	default:
		return status.Error(codes.Internal, "admin operation failed")
	}
}
