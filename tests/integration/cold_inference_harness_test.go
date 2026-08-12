//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const (
	coldModel               = "prudentia-cold-path-model"
	coldTenant              = "cold-path-tenant"
	coldAPIKey              = "cold-path-api-key-32-bytes-long"
	coldCluster             = "default"
	coldNamespace           = "inference"
	coldEngine              = "cold-engine"
	coldPodUID              = "cold-pod-uid"
	coldEndpointEpoch       = uint64(1)
	coldRecoveryEpoch       = uint64(1)
	coldManifestID          = "cold-inference-v1"
	coldManifestKeyID       = "cold-manifest-key"
	coldProviderTrustDomain = "provider.test"
	coldGatewaySPIFFEID     = "spiffe://control.test/gateway"
)

type coldPaths struct {
	root              string
	caFile            string
	gatewayClientCert string
	gatewayClientKey  string
	gatewayPublicCert string
	gatewayPublicKey  string
	schedulerCert     string
	schedulerKey      string
	proxyCert         string
	proxyKey          string
	manifestPayload   string
	manifestSignature string
	gatewayBinary     string
	schedulerBinary   string
	proxyBinary       string
}

type manifestMaterial struct {
	publicKeyBase64 string
	pin             string
	providerDigest  string
	proxyDigest     string
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate integration test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
}

func versionValue(t *testing.T, root, name string) string {
	t.Helper()
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	content, err := os.ReadFile(filepath.Join(root, ".github", "versions.env"))
	if err != nil {
		t.Fatalf("read versions.env: %v", err)
	}
	prefix := name + "="
	for _, line := range strings.Split(string(content), "\n") {
		if strings.HasPrefix(line, prefix) {
			if value := strings.TrimSpace(strings.TrimPrefix(line, prefix)); value != "" {
				return value
			}
		}
	}
	t.Fatalf("%s is not pinned", name)
	return ""
}

func buildColdBinaries(t *testing.T, root, output string) (gateway, scheduler, proxy string) {
	t.Helper()
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	build := func(name, pkg string) string {
		t.Helper()
		path := filepath.Join(output, name+extension)
		command := exec.CommandContext(t.Context(), "go", "build", "-o", path, pkg)
		command.Dir = root
		if result, err := command.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", name, err, result)
		}
		return path
	}
	return build("gateway", "./cmd/gateway"), build("scheduler", "./cmd/scheduler"), build("identity-proxy", "./cmd/identity-proxy")
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		t.Fatalf("hash %s: %v", path, err)
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func base64Key(fill byte) string {
	return base64.StdEncoding.EncodeToString(bytesOf(fill, 32))
}

func bytesOf(value byte, count int) []byte {
	result := make([]byte, count)
	for index := range result {
		result[index] = value
	}
	return result
}

func addressURL(scheme, address string) string {
	return fmt.Sprintf("%s://%s", scheme, address)
}
