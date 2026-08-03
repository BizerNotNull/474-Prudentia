package schedulergrpc

import (
	"context"
	"crypto/x509"
	"errors"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const schedulerServicePrefix = "/prudentia.scheduler.v1.SchedulerService/"

type GatewayInterceptorConfig struct {
	AllowedSPIFFEIDs []string
	MaxDeadline      time.Duration
	MaxMessageBytes  int
}

type gatewayIdentityKey struct{}

func GatewayIdentity(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(gatewayIdentityKey{}).(string)
	return value, ok
}

func NewGatewayUnaryInterceptor(config GatewayInterceptorConfig) (grpc.UnaryServerInterceptor, error) {
	if len(config.AllowedSPIFFEIDs) == 0 || config.MaxDeadline <= 0 || config.MaxMessageBytes <= 0 {
		return nil, errors.New("invalid gateway interceptor configuration")
	}
	allowed := make(map[string]struct{}, len(config.AllowedSPIFFEIDs))
	for _, raw := range config.AllowedSPIFFEIDs {
		u, err := url.Parse(raw)
		if err != nil || u.Scheme != "spiffe" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !strings.HasPrefix(u.Path, "/") {
			return nil, errors.New("invalid gateway SPIFFE identity")
		}
		allowed[u.String()] = struct{}{}
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info == nil || !strings.HasPrefix(info.FullMethod, schedulerServicePrefix) {
			return nil, status.Error(codes.PermissionDenied, "method denied")
		}
		identity, err := authenticatedSPIFFE(ctx)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "authenticated gateway required")
		}
		if _, ok := allowed[identity]; !ok {
			return nil, status.Error(codes.PermissionDenied, "gateway identity denied")
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > config.MaxDeadline {
			return nil, status.Error(codes.InvalidArgument, "bounded deadline required")
		}
		message, ok := req.(proto.Message)
		if !ok || proto.Size(message) > config.MaxMessageBytes {
			return nil, status.Error(codes.ResourceExhausted, "message limit exceeded")
		}
		return handler(context.WithValue(ctx, gatewayIdentityKey{}, identity), req)
	}, nil
}

func authenticatedSPIFFE(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", errors.New("missing peer")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return "", errors.New("unverified peer")
	}
	leaf := tlsInfo.State.VerifiedChains[0][0]
	if err := validatePeerCertificate(leaf); err != nil {
		return "", err
	}
	if len(leaf.URIs) != 1 {
		return "", errors.New("exactly one SPIFFE URI required")
	}
	u := leaf.URIs[0]
	if u.Scheme != "spiffe" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("invalid SPIFFE URI")
	}
	return u.String(), nil
}

func validatePeerCertificate(cert *x509.Certificate) error {
	if cert == nil || time.Now().Before(cert.NotBefore) || time.Now().After(cert.NotAfter) {
		return errors.New("invalid peer certificate")
	}
	return nil
}
