package config

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/BizerNotNull/474-Prudentia/internal/auth"
)

func setGatewayRequiredEnv(t *testing.T) {
	t.Helper()
	values := map[string]string{
		"PRUDENTIA_GATEWAY_API_KEY":                          "0123456789abcdef",
		"PRUDENTIA_GATEWAY_TENANT":                           "tenant-a",
		"PRUDENTIA_GATEWAY_MODELS":                           "model-a",
		"PRUDENTIA_GATEWAY_OIDC_ISSUER":                      "",
		"PRUDENTIA_GATEWAY_OIDC_AUDIENCE":                    "",
		"PRUDENTIA_GATEWAY_OIDC_JWKS_URL":                    "",
		"PRUDENTIA_GATEWAY_OIDC_TENANT_CLAIM":                "",
		"PRUDENTIA_GATEWAY_OIDC_MODELS_CLAIM":                "",
		"PRUDENTIA_SCHEDULER_SERVER_NAME":                    "scheduler.internal",
		"PRUDENTIA_SCHEDULER_CA":                             "scheduler-ca.pem",
		"PRUDENTIA_GATEWAY_TLS_CERT":                         "gateway.pem",
		"PRUDENTIA_GATEWAY_TLS_KEY":                          "gateway-key.pem",
		"PRUDENTIA_PROVIDER_CA":                              "provider-ca.pem",
		"PRUDENTIA_PROVIDER_TRUST_DOMAIN":                    "cluster.test",
		"PRUDENTIA_GATEWAY_PUBLIC_TLS_CERT":                  "public.pem",
		"PRUDENTIA_GATEWAY_PUBLIC_TLS_KEY":                   "public-key.pem",
		"PRUDENTIA_PROVIDER_MANIFEST_PAYLOAD":                "manifest.json",
		"PRUDENTIA_PROVIDER_MANIFEST_SIGNATURE":              "manifest.sig",
		"PRUDENTIA_PROVIDER_MANIFEST_KEY_ID":                 "signer-v1",
		"PRUDENTIA_PROVIDER_MANIFEST_ID":                     "provider-v1",
		"PRUDENTIA_PROVIDER_MANIFEST_PUBLIC_KEY":             encodedGatewayTestKey('p'),
		"PRUDENTIA_PROVIDER_MANIFEST_PIN":                    strings.Repeat("a", 64),
		"PRUDENTIA_GATEWAY_IDEMPOTENCY_LOOKUP_KEYS":          "1:" + encodedGatewayTestKey('l'),
		"PRUDENTIA_GATEWAY_IDEMPOTENCY_LOOKUP_WRITE_VERSION": "1",
		"PRUDENTIA_GATEWAY_REQUEST_DIGEST_KEYS":              "1:" + encodedGatewayTestKey('d'),
		"PRUDENTIA_GATEWAY_REQUEST_DIGEST_WRITE_VERSION":     "1",
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
}

func encodedGatewayTestKey(value byte) string {
	return base64.StdEncoding.EncodeToString([]byte(strings.Repeat(string(value), 32)))
}

func TestLoadGatewayAcceptsOIDCWithoutAPIKey(t *testing.T) {
	setGatewayRequiredEnv(t)
	t.Setenv("PRUDENTIA_GATEWAY_API_KEY", "")
	t.Setenv("PRUDENTIA_GATEWAY_TENANT", "")
	t.Setenv("PRUDENTIA_GATEWAY_MODELS", "")
	t.Setenv("PRUDENTIA_GATEWAY_OIDC_ISSUER", "https://issuer.example/realms/prudentia")
	t.Setenv("PRUDENTIA_GATEWAY_OIDC_AUDIENCE", "prudentia-gateway")
	t.Setenv("PRUDENTIA_GATEWAY_OIDC_JWKS_URL", "https://issuer.example/realms/prudentia/keys")
	t.Setenv("PRUDENTIA_GATEWAY_OIDC_TENANT_CLAIM", "organization")
	t.Setenv("PRUDENTIA_GATEWAY_OIDC_MODELS_CLAIM", "allowed_models")

	cfg, err := LoadGatewayFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.OIDCIssuers) != 1 {
		t.Fatalf("got %d OIDC issuers, want 1", len(cfg.OIDCIssuers))
	}
	issuer := cfg.OIDCIssuers[0]
	if issuer.TenantClaim != "organization" || issuer.ModelsClaim != "allowed_models" {
		t.Fatalf("unexpected claim mapping: %#v", issuer)
	}
	if _, err := auth.NewAuthenticatorWithOIDC(nil, cfg.OIDCIssuers); err != nil {
		t.Fatalf("loaded OIDC configuration cannot build authenticator: %v", err)
	}
}

func TestLoadGatewayRejectsPartialOIDCConfiguration(t *testing.T) {
	setGatewayRequiredEnv(t)
	t.Setenv("PRUDENTIA_GATEWAY_API_KEY", "")
	t.Setenv("PRUDENTIA_GATEWAY_TENANT", "")
	t.Setenv("PRUDENTIA_GATEWAY_MODELS", "")
	t.Setenv("PRUDENTIA_GATEWAY_OIDC_ISSUER", "https://issuer.example")

	if _, err := LoadGatewayFromEnv(); err == nil {
		t.Fatal("gateway accepted partial OIDC configuration")
	}
}

func TestLoadGatewayRequiresCredentialSource(t *testing.T) {
	setGatewayRequiredEnv(t)
	t.Setenv("PRUDENTIA_GATEWAY_API_KEY", "")
	t.Setenv("PRUDENTIA_GATEWAY_TENANT", "")
	t.Setenv("PRUDENTIA_GATEWAY_MODELS", "")

	if _, err := LoadGatewayFromEnv(); err == nil {
		t.Fatal("gateway accepted missing credential sources")
	}
}
