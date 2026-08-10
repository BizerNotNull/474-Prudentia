package config

import (
	"errors"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

const defaultProxyMaxRequestBytes int64 = 2 << 20

type IdentityProxy struct {
	ListenAddress           string
	Upstream                *url.URL
	TLSCertFile             string
	TLSKeyFile              string
	ServerCAFile            string
	GatewayClientCAFile     string
	AllowedGatewaySPIFFEIDs []string
	Identity                domain.WorkloadIdentity
	ManifestID              string
	ProviderImageDigest     string
	ProxyImageDigest        string
	MaxRequestBytes         int64
}

func LoadIdentityProxyFromEnv() (IdentityProxy, error) {
	rawUpstream := strings.TrimSpace(os.Getenv("PRUDENTIA_PROXY_UPSTREAM"))
	if rawUpstream == "" {
		rawUpstream = "http://127.0.0.1:8000"
	}
	upstream, err := url.Parse(rawUpstream)
	if err != nil || upstream.Scheme != "http" || upstream.User != nil || upstream.RawQuery != "" || upstream.Fragment != "" || upstream.Path != "" || !isLoopbackHost(upstream.Hostname()) || upstream.Port() == "" {
		return IdentityProxy{}, errors.New("proxy upstream must be an explicit loopback HTTP endpoint")
	}
	cfg := IdentityProxy{
		ListenAddress:       strings.TrimSpace(os.Getenv("PRUDENTIA_PROXY_LISTEN")),
		Upstream:            upstream,
		TLSCertFile:         strings.TrimSpace(os.Getenv("PRUDENTIA_PROXY_TLS_CERT")),
		TLSKeyFile:          strings.TrimSpace(os.Getenv("PRUDENTIA_PROXY_TLS_KEY")),
		ServerCAFile:        strings.TrimSpace(os.Getenv("PRUDENTIA_PROXY_SERVER_CA")),
		GatewayClientCAFile: strings.TrimSpace(os.Getenv("PRUDENTIA_PROXY_GATEWAY_CLIENT_CA")),
		ManifestID:          strings.TrimSpace(os.Getenv("PRUDENTIA_PROXY_MANIFEST_ID")),
		ProviderImageDigest: strings.TrimSpace(os.Getenv("PRUDENTIA_PROXY_PROVIDER_IMAGE_DIGEST")),
		ProxyImageDigest:    strings.TrimSpace(os.Getenv("PRUDENTIA_PROXY_IMAGE_DIGEST")),
		MaxRequestBytes:     defaultProxyMaxRequestBytes,
	}
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = ":9443"
	}
	for _, raw := range strings.Split(os.Getenv("PRUDENTIA_PROXY_ALLOWED_GATEWAY_SPIFFE_IDS"), ",") {
		if value := strings.TrimSpace(raw); value != "" {
			cfg.AllowedGatewaySPIFFEIDs = append(cfg.AllowedGatewaySPIFFEIDs, value)
		}
	}
	if raw := strings.TrimSpace(os.Getenv("PRUDENTIA_PROXY_MAX_REQUEST_BYTES")); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 1 || value > 16<<20 {
			return IdentityProxy{}, errors.New("invalid proxy request body limit")
		}
		cfg.MaxRequestBytes = value
	}
	endpointEpoch, endpointErr := strconv.ParseUint(strings.TrimSpace(os.Getenv("PRUDENTIA_ENDPOINT_EPOCH")), 10, 64)
	recoveryEpoch, recoveryErr := strconv.ParseUint(strings.TrimSpace(os.Getenv("PRUDENTIA_RECOVERY_EPOCH")), 10, 64)
	identity, identityErr := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{
		Cluster: strings.TrimSpace(os.Getenv("PRUDENTIA_CLUSTER")), Namespace: strings.TrimSpace(os.Getenv("POD_NAMESPACE")),
		LogicalEngine: strings.TrimSpace(os.Getenv("PRUDENTIA_LOGICAL_ENGINE")), PodUID: strings.TrimSpace(os.Getenv("POD_UID")),
		EndpointEpoch: endpointEpoch, RecoveryEpoch: recoveryEpoch,
	})
	if endpointErr != nil || recoveryErr != nil || identityErr != nil {
		return IdentityProxy{}, errors.New("invalid exact proxy workload identity")
	}
	cfg.Identity = identity
	if _, _, err := net.SplitHostPort(cfg.ListenAddress); err != nil {
		return IdentityProxy{}, errors.New("invalid proxy listen address")
	}
	if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" || cfg.ServerCAFile == "" || cfg.GatewayClientCAFile == "" || len(cfg.AllowedGatewaySPIFFEIDs) == 0 {
		return IdentityProxy{}, errors.New("proxy mTLS configuration is required")
	}
	if cfg.ManifestID == "" || !validDigest(cfg.ProviderImageDigest) || !validDigest(cfg.ProxyImageDigest) {
		return IdentityProxy{}, errors.New("proxy manifest binding is required")
	}
	return cfg, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}

func validDigest(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, char := range value[len("sha256:"):] {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}
