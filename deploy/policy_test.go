package deploy_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func artifact(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.ReplaceAll(string(body), "\r\n", "\n")
}

func TestImagesAreImmutableAndNeverLatest(t *testing.T) {
	imageLine := regexp.MustCompile(`(?m)^\s*image:\s*([^\s]+)`)
	digest := regexp.MustCompile(`@sha256:[0-9a-f]{64}$`)
	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || (filepath.Ext(path) != ".yaml" && filepath.Ext(path) != ".yml") {
			return err
		}
		for _, match := range imageLine.FindAllStringSubmatch(artifact(t, path), -1) {
			if strings.Contains(match[1], ":latest") || !digest.MatchString(match[1]) {
				t.Errorf("%s has mutable image %q", path, match[1])
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRequestAndAdminListenersAreSeparated(t *testing.T) {
	workloads := artifact(t, "base/workloads.yaml")
	for _, required := range []string{"name: scheduler-admin", "port: 9444", "name: scheduler", "port: 9443", "automountServiceAccountToken: false"} {
		if !strings.Contains(workloads, required) {
			t.Errorf("workload artifact lacks %q", required)
		}
	}
	gatewayStart := strings.Index(workloads, "kind: Deployment\nmetadata: {name: gateway")
	schedulerStart := strings.Index(workloads, "kind: Deployment\nmetadata: {name: scheduler")
	if gatewayStart < 0 || schedulerStart < 0 || strings.Contains(workloads[gatewayStart:schedulerStart], "POSTGRES_DSN") {
		t.Error("gateway must not receive a PostgreSQL credential")
	}
}

func TestRBACAdmissionAndNetworkIsolationAreFailClosed(t *testing.T) {
	rbac := artifact(t, "policy/rbac-admission.yaml")
	for _, required := range []string{"resourceNames: [prudentia-controller]", "prudentia.io/operation-token", "prudentia.io/operation-generation", "prudentia.io/admission-closed", "preconditions.uid", "preconditions.resourceVersion", "failurePolicy: Fail"} {
		if !strings.Contains(rbac, required) {
			t.Errorf("RBAC/admission artifact lacks %q", required)
		}
	}
	if strings.Contains(rbac, "resources: [secrets]") || strings.Contains(rbac, "verbs: [\"*\"]") {
		t.Error("controller RBAC grants secrets or wildcard verbs")
	}
	network := artifact(t, "policy/network-policies.yaml")
	for _, required := range []string{"name: default-deny", "podSelector: {}", "name: scheduler-request", "port: 9444", "name: prudentia-postgresql-boundary"} {
		if !strings.Contains(network, required) {
			t.Errorf("network policy lacks %q", required)
		}
	}
}

func TestRecoveryCannotReopenWithoutFleetProof(t *testing.T) {
	jobs := artifact(t, "recovery/jobs.yaml")
	script := artifact(t, "../scripts/recover.sh")
	for _, required := range []string{"suspend: true", "infrastructure fence", "zero old identities", "fleet-rebuild.json", "backoffLimit: 0"} {
		if !strings.Contains(jobs, required) {
			t.Errorf("recovery manifests lack %q", required)
		}
	}
	for _, required := range []string{"ingress=closed dispatch=closed", "admission_state='fenced'", "dispatch_state='fenced'", ".oldIdentityCount == 0", ".registryRebuilt == true", ".capacityRebuilt == true", "matching closed recovery fence not found", "fleet rebuild incomplete"} {
		if !strings.Contains(script, required) {
			t.Errorf("recovery script lacks %q", required)
		}
	}
}

func TestIdentityCRDDatabaseAndAlertsCarrySafetyContracts(t *testing.T) {
	identity := artifact(t, "identity/proxy-capability-example.yaml")
	crd := artifact(t, "crd/inferencefleets.yaml")
	roles := artifact(t, "postgres/roles.sql")
	alerts := artifact(t, "observability/alerts.yaml")
	for _, required := range []string{"PodMeta.UID", "require-recovery-epoch", "fleetQuiescenceProof", "127.0.0.1:8000"} {
		if !strings.Contains(identity, required) {
			t.Errorf("identity artifact lacks %q", required)
		}
	}
	if !strings.Contains(crd, "capabilityManifestRef") || !strings.Contains(crd, "proofDigest") || !strings.Contains(crd, "admissionClosed") {
		t.Error("CRD lacks capability, proof, or admission linkage")
	}
	if strings.Contains(roles, "prudentia_gateway NOLOGIN") || !strings.Contains(roles, "Deliberately no prudentia_gateway") {
		t.Error("PostgreSQL roles do not exclude gateway")
	}
	for _, alert := range []string{"PrudentiaUnsafeDebtOverride", "PrudentiaRecoveryFenceClosed", "PrudentiaOperationBarrierStalled"} {
		if !strings.Contains(alerts, alert) {
			t.Errorf("missing alert %s", alert)
		}
	}
}
