package vllm

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

func identityProxyFixture(t *testing.T, upstream *url.URL) (*IdentityProxy, IdentityProxyConfig) {
	t.Helper()
	identity := testIdentity(t)
	image := "sha256:" + strings.Repeat("a", 64)
	proxyDigest := "sha256:" + strings.Repeat("b", 64)
	certificate, _, roots := certificateFixture(t, identity, PeerIdentityClaims{
		PodUID: identity.PodUID(), EndpointEpoch: identity.EndpointEpoch(), RecoveryEpoch: identity.RecoveryEpoch(),
		ManifestID: "manifest", ImageDigest: image, ProxyDigest: proxyDigest,
	})
	config := IdentityProxyConfig{
		Upstream: upstream, Certificate: certificate, ServerRoots: roots, GatewayClientRoots: roots,
		AllowedGatewaySPIFFEIDs: []string{"spiffe://example.test/prudentia/gateway"}, Identity: identity,
		ManifestID: "manifest", ProviderImageDigest: image, ProxyImageDigest: proxyDigest, MaxRequestBytes: 32,
	}
	proxy, err := NewIdentityProxy(config)
	if err != nil {
		t.Fatal(err)
	}
	return proxy, config
}

func TestIdentityProxyForwardsOnlyBoundedAllowlistedRequests(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()
	upstreamURL, _ := url.Parse(upstream.URL)
	proxy, _ := identityProxyFixture(t, upstreamURL)

	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	response := httptest.NewRecorder()
	proxy.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("forward status/calls = %d/%d", response.Code, calls.Load())
	}

	request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(strings.Repeat("x", 33)))
	response = httptest.NewRecorder()
	proxy.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge || calls.Load() != 1 {
		t.Fatalf("oversized status/calls = %d/%d", response.Code, calls.Load())
	}

	request = httptest.NewRequest(http.MethodPost, "/unknown", strings.NewReader("secret"))
	response = httptest.NewRecorder()
	proxy.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound || calls.Load() != 1 {
		t.Fatalf("unknown route status/calls = %d/%d", response.Code, calls.Load())
	}
}

func TestIdentityProxyRejectsManifestAndGatewayIdentityMismatch(t *testing.T) {
	upstreamURL, _ := url.Parse("http://127.0.0.1:8000")
	_, config := identityProxyFixture(t, upstreamURL)
	config.ManifestID = "other"
	if _, err := NewIdentityProxy(config); err == nil {
		t.Fatal("proxy accepted certificate for another manifest")
	}

	allowedURI, _ := url.Parse("spiffe://example.test/prudentia/gateway")
	allowedLeaf := &x509.Certificate{KeyUsage: x509.KeyUsageDigitalSignature, URIs: []*url.URL{allowedURI}}
	allowed := map[string]struct{}{allowedURI.String(): {}}
	if err := verifyAllowedGateway(tlsState(allowedLeaf), allowed); err != nil {
		t.Fatalf("allowed gateway rejected: %v", err)
	}
	wrongURI, _ := url.Parse("spiffe://example.test/prudentia/other")
	wrongLeaf := &x509.Certificate{KeyUsage: x509.KeyUsageDigitalSignature, URIs: []*url.URL{wrongURI}}
	if err := verifyAllowedGateway(tlsState(wrongLeaf), allowed); err == nil {
		t.Fatal("unauthorized gateway identity accepted")
	}
}

func tlsState(leaf *x509.Certificate) tls.ConnectionState {
	return tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf}}}
}
