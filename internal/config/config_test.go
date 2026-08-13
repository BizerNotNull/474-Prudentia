package config

import (
	"strings"
	"testing"
)

func TestGatewayRequiresPublicTLSAndPinnedManifest(t *testing.T) {
	setGatewayRequiredEnv(t)
	if _, err := LoadGatewayFromEnv(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PRUDENTIA_PROVIDER_MANIFEST_PIN", "")
	if _, err := LoadGatewayFromEnv(); err == nil {
		t.Fatal("gateway accepted an unpinned provider manifest")
	}
}

func TestSchedulerRequiresSeparateAdminIdentityPolicyAndRetainedKeys(t *testing.T) {
	values := map[string]string{
		"PRUDENTIA_DATABASE_URL": "postgres://scheduler.invalid/db", "PRUDENTIA_SCHEDULER_TLS_CERT": "scheduler.pem",
		"PRUDENTIA_SCHEDULER_TLS_KEY": "scheduler-key.pem", "PRUDENTIA_SCHEDULER_CLIENT_CA": "gateway-ca.pem",
		"PRUDENTIA_GATEWAY_SPIFFE_ID":        "spiffe://cluster.test/ns/system/sa/gateway",
		"PRUDENTIA_SCHEDULER_CAPABILITY_KEY": encodedGatewayTestKey('k'), "PRUDENTIA_SCHEDULER_ADMIN_CLIENT_CA": "operator-ca.pem",
		"PRUDENTIA_SCHEDULER_ADMIN_TRUST_DOMAIN": "cluster.test", "PRUDENTIA_SCHEDULER_ADMIN_PATH_PREFIXES": "/operator/",
		"PRUDENTIA_SCHEDULER_CAPABILITY_KEKS":            "1:" + encodedGatewayTestKey('a') + ",2:" + encodedGatewayTestKey('b'),
		"PRUDENTIA_SCHEDULER_CAPABILITY_COMPARISON_KEYS": "1:" + encodedGatewayTestKey('c') + ",2:" + encodedGatewayTestKey('d'),
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
	cfg, err := LoadSchedulerFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.CapabilityKEKs) != 2 || cfg.AdminListenAddress == cfg.ListenAddress {
		t.Fatal("scheduler did not retain keys or separate listeners")
	}
	t.Setenv("PRUDENTIA_SCHEDULER_ADMIN_PATH_PREFIXES", "")
	if _, err := LoadSchedulerFromEnv(); err == nil {
		t.Fatal("scheduler accepted missing admin authorization policy")
	}
}

func TestControllerRequiresExactProviderEvidenceConfiguration(t *testing.T) {
	values := map[string]string{
		"PRUDENTIA_DATABASE_URL": "postgres://controller.invalid/db", "PRUDENTIA_CLUSTER": "cluster-a",
		"PRUDENTIA_CONTROLLER_NAMESPACE": "models", "PRUDENTIA_CONTROLLER_LEASE_NAMESPACE": "models", "HOSTNAME": "controller-0",
		"PRUDENTIA_CONTROLLER_TLS_CERT": "controller.pem", "PRUDENTIA_CONTROLLER_TLS_KEY": "controller-key.pem",
		"PRUDENTIA_PROVIDER_CA": "provider-ca.pem", "PRUDENTIA_PROVIDER_TRUST_DOMAIN": "cluster.test",
		"PRUDENTIA_PROVIDER_MANIFEST_PAYLOAD": "manifest.json", "PRUDENTIA_PROVIDER_MANIFEST_SIGNATURE": "manifest.sig",
		"PRUDENTIA_PROVIDER_MANIFEST_KEY_ID": "signer-v1", "PRUDENTIA_PROVIDER_MANIFEST_ID": "provider-v1",
		"PRUDENTIA_PROVIDER_MANIFEST_PUBLIC_KEY": encodedGatewayTestKey('p'), "PRUDENTIA_PROVIDER_MANIFEST_PIN": strings.Repeat("b", 64),
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
	if _, err := LoadControllerFromEnv(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PRUDENTIA_PROVIDER_CA", "")
	if _, err := LoadControllerFromEnv(); err == nil {
		t.Fatal("controller accepted missing provider trust roots")
	}
}
