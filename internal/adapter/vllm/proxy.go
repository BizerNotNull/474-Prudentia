package vllm

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

const maxProxyHeaderBytes = 32 << 10

type IdentityProxyConfig struct {
	Upstream                *url.URL
	Certificate             tls.Certificate
	ServerRoots             *x509.CertPool
	GatewayClientRoots      *x509.CertPool
	AllowedGatewaySPIFFEIDs []string
	Identity                domain.WorkloadIdentity
	ManifestID              string
	ProviderImageDigest     string
	ProxyImageDigest        string
	MaxRequestBytes         int64
}

type IdentityProxy struct {
	handler   http.Handler
	tlsConfig *tls.Config
}

func LoadIdentityProxyConfig(certFile, keyFile, serverCAFile, gatewayClientCAFile string, base IdentityProxyConfig) (IdentityProxyConfig, error) {
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return IdentityProxyConfig{}, fmt.Errorf("load proxy TLS identity: %w", err)
	}
	serverRoots, err := loadProxyCertPool(serverCAFile)
	if err != nil {
		return IdentityProxyConfig{}, fmt.Errorf("load proxy server CA: %w", err)
	}
	gatewayRoots, err := loadProxyCertPool(gatewayClientCAFile)
	if err != nil {
		return IdentityProxyConfig{}, fmt.Errorf("load gateway client CA: %w", err)
	}
	base.Certificate = certificate
	base.ServerRoots = serverRoots
	base.GatewayClientRoots = gatewayRoots
	return base, nil
}

func loadProxyCertPool(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, errors.New("certificate file contains no CA certificate")
	}
	return pool, nil
}

func NewIdentityProxy(config IdentityProxyConfig) (*IdentityProxy, error) {
	if config.Upstream == nil || config.Upstream.Scheme != "http" || config.Upstream.User != nil || config.Upstream.RawQuery != "" || config.Upstream.Fragment != "" || config.Upstream.Path != "" || config.MaxRequestBytes < 1 || config.MaxRequestBytes > 16<<20 || config.ServerRoots == nil || config.GatewayClientRoots == nil || len(config.Certificate.Certificate) == 0 || len(config.AllowedGatewaySPIFFEIDs) == 0 {
		return nil, errors.New("invalid identity proxy configuration")
	}
	allowed := make(map[string]struct{}, len(config.AllowedGatewaySPIFFEIDs))
	for _, raw := range config.AllowedGatewaySPIFFEIDs {
		identity, err := url.Parse(raw)
		if err != nil || identity.Scheme != "spiffe" || identity.Host == "" || identity.User != nil || identity.RawQuery != "" || identity.Fragment != "" || !strings.HasPrefix(identity.Path, "/") {
			return nil, errors.New("invalid allowed gateway identity")
		}
		allowed[identity.String()] = struct{}{}
	}
	leaf, chains, err := verifyConfiguredProxyCertificate(config.Certificate, config.ServerRoots)
	if err != nil {
		return nil, err
	}
	claims, err := verifyPeerIdentity(tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: chains}, config.Identity)
	if err != nil {
		return nil, fmt.Errorf("validate configured exact proxy identity: %w", err)
	}
	if claims.ManifestID != config.ManifestID || claims.ImageDigest != config.ProviderImageDigest || claims.ProxyDigest != config.ProxyImageDigest {
		return nil, errors.New("configured proxy certificate manifest binding mismatch")
	}
	reverse := &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(config.Upstream)
			request.Out.Host = config.Upstream.Host
			request.Out.Header.Del("Forwarded")
			request.Out.Header.Del("X-Forwarded-For")
			request.Out.Header.Del("X-Forwarded-Host")
			request.Out.Header.Del("X-Forwarded-Proto")
		},
		Transport: &http.Transport{Proxy: nil, DisableCompression: true, MaxIdleConns: 16, MaxIdleConnsPerHost: 16},
		ErrorHandler: func(writer http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(writer, "provider unavailable", http.StatusBadGateway)
		},
	}
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !allowedProxyRoute(request.Method, request.URL.Path) || request.URL.RawQuery != "" {
			http.NotFound(writer, request)
			return
		}
		if request.ContentLength > config.MaxRequestBytes {
			http.Error(writer, "request too large", http.StatusRequestEntityTooLarge)
			return
		}
		if request.Body != nil {
			body, err := io.ReadAll(io.LimitReader(request.Body, config.MaxRequestBytes+1))
			request.Body.Close()
			if err != nil || int64(len(body)) > config.MaxRequestBytes {
				http.Error(writer, "request too large", http.StatusRequestEntityTooLarge)
				return
			}
			request.Body = io.NopCloser(bytes.NewReader(body))
			request.ContentLength = int64(len(body))
		}
		reverse.ServeHTTP(writer, request)
	})
	return &IdentityProxy{handler: handler, tlsConfig: &tls.Config{
		Certificates: []tls.Certificate{config.Certificate}, ClientCAs: config.GatewayClientRoots,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13,
		VerifyConnection: func(state tls.ConnectionState) error { return verifyAllowedGateway(state, allowed) },
	}}, nil
}

func verifyConfiguredProxyCertificate(certificate tls.Certificate, roots *x509.CertPool) (*x509.Certificate, [][]*x509.Certificate, error) {
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return nil, nil, errors.New("parse proxy leaf certificate")
	}
	intermediates := x509.NewCertPool()
	for _, raw := range certificate.Certificate[1:] {
		value, err := x509.ParseCertificate(raw)
		if err != nil {
			return nil, nil, errors.New("parse proxy certificate chain")
		}
		intermediates.AddCert(value)
	}
	chains, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
	if err != nil {
		return nil, nil, errors.New("verify proxy certificate chain")
	}
	return leaf, chains, nil
}

func verifyAllowedGateway(state tls.ConnectionState, allowed map[string]struct{}) error {
	if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 || !state.PeerCertificates[0].Equal(state.VerifiedChains[0][0]) {
		return errors.New("gateway trust chain not verified")
	}
	leaf := state.PeerCertificates[0]
	if leaf.IsCA || leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0 || len(leaf.URIs) != 1 {
		return errors.New("gateway certificate violates proxy policy")
	}
	if _, ok := allowed[leaf.URIs[0].String()]; !ok {
		return errors.New("gateway identity is not authorized")
	}
	return nil
}

func allowedProxyRoute(method, path string) bool {
	switch path {
	case "/v1/chat/completions", "/v1/prudentia/terminate":
		return method == http.MethodPost
	case "/health", "/metrics", "/v1/models":
		return method == http.MethodGet
	default:
		return false
	}
}

func (p *IdentityProxy) Handler() http.Handler  { return p.handler }
func (p *IdentityProxy) TLSConfig() *tls.Config { return p.tlsConfig.Clone() }
func ProxyMaxHeaderBytes() int                  { return maxProxyHeaderBytes }
