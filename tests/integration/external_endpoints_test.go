//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func requiredEnv(t *testing.T, name, purpose string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Skipf("missing prerequisite: set %s to %s", name, purpose)
	}
	return value
}

func TestConfiguredPostgreSQLLedgerEndpoint(t *testing.T) {
	dsn := requiredEnv(t, "PRUDENTIA_INTEGRATION_POSTGRES_DSN", "a disposable migrated PostgreSQL ledger DSN")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect configured PostgreSQL: %v", err)
	}
	defer conn.Close(context.Background())
	var databaseTime time.Time
	if err := conn.QueryRow(ctx, "select transaction_timestamp()").Scan(&databaseTime); err != nil {
		t.Fatalf("query database authority time: %v", err)
	}
	if databaseTime.IsZero() {
		t.Fatal("configured PostgreSQL returned a zero authority timestamp")
	}
}

func TestConfiguredKindAPIServer(t *testing.T) {
	kubeconfig := requiredEnv(t, "PRUDENTIA_INTEGRATION_KUBECONFIG", "the kubeconfig path for a disposable kind cluster")
	contextName := requiredEnv(t, "PRUDENTIA_INTEGRATION_KIND_CONTEXT", "the exact kind kubeconfig context name")
	kubectl, err := exec.LookPath("kubectl")
	if err != nil {
		t.Skip("missing prerequisite: install kubectl to exercise the configured kind API server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, kubectl, "--kubeconfig", kubeconfig, "--context", contextName, "version", "-o", "json").CombinedOutput()
	if err != nil {
		t.Fatalf("query configured kind API server: %v: %.1024s", err, output)
	}
	var version struct {
		ServerVersion struct {
			GitVersion string `json:"gitVersion"`
		} `json:"serverVersion"`
	}
	if err := json.Unmarshal(output, &version); err != nil {
		t.Fatalf("decode kind server version: %v", err)
	}
	if version.ServerVersion.GitVersion == "" {
		t.Fatal("configured kind API server omitted its version")
	}
}

func TestConfiguredSPIFFEWorkloadEndpointReachable(t *testing.T) {
	endpoint := requiredEnv(t, "PRUDENTIA_INTEGRATION_SPIFFE_SOCKET", "the SPIRE Workload API Unix socket path")
	connection, err := net.DialTimeout("unix", endpoint, 5*time.Second)
	if err != nil {
		t.Fatalf("dial configured SPIFFE Workload API socket: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close SPIFFE socket: %v", err)
	}
}

type countingTransport struct {
	base  http.RoundTripper
	posts atomic.Int32
}

func (t *countingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method == http.MethodPost {
		t.posts.Add(1)
	}
	return t.base.RoundTrip(request)
}

func configuredHTTPClient(t *testing.T) (*http.Client, *countingTransport) {
	t.Helper()
	roots, err := x509.SystemCertPool()
	if err != nil {
		t.Fatalf("load system roots: %v", err)
	}
	transport := &countingTransport{base: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}, DisableCompression: true, ResponseHeaderTimeout: 30 * time.Second}}
	return &http.Client{Transport: transport, Timeout: time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, transport
}

func TestConfiguredVLLMProtocolEndpoint(t *testing.T) {
	baseURL := strings.TrimRight(requiredEnv(t, "PRUDENTIA_INTEGRATION_VLLM_URL", "the HTTPS URL of the explicitly pinned vLLM or llm-d-inference-sim endpoint"), "/")
	model := requiredEnv(t, "PRUDENTIA_INTEGRATION_VLLM_MODEL", "the model name configured on that endpoint")
	client, counter := configuredHTTPClient(t)
	payload, _ := json.Marshal(map[string]any{"model": model, "messages": []map[string]string{{"role": "user", "content": "protocol probe"}}, "max_tokens": 1, "stream": false})
	request, err := http.NewRequest(http.MethodPost, baseURL+"/v1/chat/completions", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("call configured vLLM endpoint: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, (2<<20)+1))
	if err != nil {
		t.Fatalf("read bounded vLLM response: %v", err)
	}
	if len(body) > 2<<20 {
		t.Fatal("configured vLLM response exceeded the 2 MiB harness bound")
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("vLLM status = %d, body = %.512s", response.StatusCode, body)
	}
	var envelope struct {
		Choices []json.RawMessage `json:"choices"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || len(envelope.Choices) == 0 {
		t.Fatalf("vLLM response is not a bounded chat-completion envelope: %v", err)
	}
	if counter.posts.Load() != 1 {
		t.Fatalf("provider POST count = %d, want exactly one", counter.posts.Load())
	}
}

func TestConfiguredGPUProviderAvailabilityOnly(t *testing.T) {
	baseURL := strings.TrimRight(requiredEnv(t, "PRUDENTIA_INTEGRATION_GPU_VLLM_URL", "the HTTPS URL of the explicitly pinned GPU-backed vLLM endpoint"), "/")
	client, counter := configuredHTTPClient(t)
	response, err := client.Get(baseURL + "/v1/models")
	if err != nil {
		t.Fatalf("query GPU provider model inventory: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || len(body) > 1<<20 {
		t.Fatalf("GPU provider inventory status/bytes = %d/%d", response.StatusCode, len(body))
	}
	if counter.posts.Load() != 0 {
		t.Fatalf("availability probe unexpectedly issued %d POST(s)", counter.posts.Load())
	}
	t.Log("availability and protocol reachability only; this test does not claim model correctness, batching, performance, or KV-cache semantics")
}

func TestConfiguredTwoEngineMoverEndpoints(t *testing.T) {
	source := requiredEnv(t, "PRUDENTIA_INTEGRATION_MOVER_SOURCE_URL", "the authenticated source-engine health/control endpoint")
	destination := requiredEnv(t, "PRUDENTIA_INTEGRATION_MOVER_DESTINATION_URL", "the authenticated destination-engine health/control endpoint")
	bearer := requiredEnv(t, "PRUDENTIA_INTEGRATION_MOVER_BEARER_TOKEN", "a short-lived credential accepted by both disposable mover endpoints")
	flowEvidence := requiredEnv(t, "PRUDENTIA_INTEGRATION_MOVER_FLOW_EVIDENCE", "a harness-produced packet/flow evidence artifact path")
	client, _ := configuredHTTPClient(t)
	for label, endpoint := range map[string]string{"source": source, "destination": destination} {
		if !strings.HasPrefix(endpoint, "https://") {
			t.Fatalf("%s endpoint must use HTTPS", label)
		}
		request, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+bearer)
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("exercise configured %s mover endpoint: %v", label, err)
		}
		_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
			t.Fatalf("%s mover endpoint status/read/close = %d/%v/%v", label, response.StatusCode, readErr, closeErr)
		}
	}
	info, err := os.Stat(flowEvidence)
	if err != nil {
		t.Fatalf("read mover flow-evidence artifact: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("mover flow-evidence artifact is empty")
	}
	t.Log(fmt.Sprintf("both external mover endpoints were exercised and a non-empty flow artifact exists; this harness does not infer KV correctness or bypass from file presence alone: %s", flowEvidence))
}
