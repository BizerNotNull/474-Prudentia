//go:build integration

package integration_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/adapter/vllm"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
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

type processHandle struct {
	name   string
	cancel context.CancelFunc
	done   chan struct{}
	logs   *boundedLog
	mu     sync.Mutex
	err    error
}

func (p *processHandle) finish(err error) {
	p.mu.Lock()
	p.err = err
	p.mu.Unlock()
	close(p.done)
}

func (p *processHandle) waitError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

type boundedLog struct {
	mu    sync.Mutex
	limit int
	data  []byte
}

func (b *boundedLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	remaining := b.limit - len(b.data)
	if remaining > 0 {
		if len(p) < remaining {
			remaining = len(p)
		}
		b.data = append(b.data, p[:remaining]...)
	}
	return len(p), nil
}

func (b *boundedLog) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(append([]byte(nil), b.data...))
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

func newColdPaths(t *testing.T, providerDigest string) (coldPaths, manifestMaterial) {
	t.Helper()
	root := repositoryRoot(t)
	directory := t.TempDir()
	gateway, scheduler, proxy := buildColdBinaries(t, root, directory)
	paths := coldPaths{
		root:              root,
		caFile:            filepath.Join(directory, "ca.pem"),
		gatewayClientCert: filepath.Join(directory, "gateway-client.pem"),
		gatewayClientKey:  filepath.Join(directory, "gateway-client-key.pem"),
		gatewayPublicCert: filepath.Join(directory, "gateway-public.pem"),
		gatewayPublicKey:  filepath.Join(directory, "gateway-public-key.pem"),
		schedulerCert:     filepath.Join(directory, "scheduler.pem"),
		schedulerKey:      filepath.Join(directory, "scheduler-key.pem"),
		proxyCert:         filepath.Join(directory, "proxy.pem"),
		proxyKey:          filepath.Join(directory, "proxy-key.pem"),
		manifestPayload:   filepath.Join(directory, "manifest.json"),
		manifestSignature: filepath.Join(directory, "manifest.sig"),
		gatewayBinary:     gateway,
		schedulerBinary:   scheduler,
		proxyBinary:       proxy,
	}
	proxyDigest := sha256File(t, proxy)
	ca, caKey := writeTestCA(t, paths.caFile)
	writeLeaf(t, ca, caKey, paths.schedulerCert, paths.schedulerKey, leafSpec{
		commonName: "scheduler.test", dnsNames: []string{"scheduler.test"}, serverAuth: true,
	})
	gatewayURI := mustParseURI(t, coldGatewaySPIFFEID)
	writeLeaf(t, ca, caKey, paths.gatewayClientCert, paths.gatewayClientKey, leafSpec{
		commonName: "cold-gateway", uris: []*url.URL{gatewayURI}, clientAuth: true,
	})
	writeLeaf(t, ca, caKey, paths.gatewayPublicCert, paths.gatewayPublicKey, leafSpec{
		commonName: "localhost", dnsNames: []string{"localhost"}, ipAddresses: []net.IP{net.ParseIP("127.0.0.1")}, serverAuth: true,
	})
	identity := coldIdentity(t)
	proxyURI, err := identity.SPIFFEID(coldProviderTrustDomain)
	if err != nil {
		t.Fatalf("create proxy SPIFFE ID: %v", err)
	}
	claims, err := json.Marshal(vllm.PeerIdentityClaims{
		PodUID: coldPodUID, EndpointEpoch: coldEndpointEpoch, RecoveryEpoch: coldRecoveryEpoch,
		ManifestID: coldManifestID, ImageDigest: providerDigest, ProxyDigest: proxyDigest,
	})
	if err != nil {
		t.Fatalf("marshal proxy claims: %v", err)
	}
	writeLeaf(t, ca, caKey, paths.proxyCert, paths.proxyKey, leafSpec{
		commonName: "cold-identity-proxy", ipAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		uris: []*url.URL{proxyURI}, serverAuth: true,
		extraExtensions: []pkix.Extension{{Id: vllm.PeerClaimsExtensionOID(), Critical: false, Value: claims}},
	})
	manifest := writeManifest(t, paths, providerDigest, proxyDigest)
	return paths, manifest
}

type leafSpec struct {
	commonName      string
	dnsNames        []string
	ipAddresses     []net.IP
	uris            []*url.URL
	serverAuth      bool
	clientAuth      bool
	extraExtensions []pkix.Extension
}

func writeTestCA(t *testing.T, path string) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Prudentia cold-path test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	certificate, err := x509.ParseCertificate(raw)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	writePEM(t, path, "CERTIFICATE", raw)
	return certificate, key
}

func writeLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, certPath, keyPath string, spec leafSpec) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate %s key: %v", spec.commonName, err)
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		t.Fatalf("generate %s serial: %v", spec.commonName, err)
	}
	usages := make([]x509.ExtKeyUsage, 0, 2)
	if spec.serverAuth {
		usages = append(usages, x509.ExtKeyUsageServerAuth)
	}
	if spec.clientAuth {
		usages = append(usages, x509.ExtKeyUsageClientAuth)
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: spec.commonName},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(12 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usages,
		DNSNames: append([]string(nil), spec.dnsNames...), IPAddresses: append([]net.IP(nil), spec.ipAddresses...),
		URIs: append([]*url.URL(nil), spec.uris...), ExtraExtensions: append([]pkix.Extension(nil), spec.extraExtensions...),
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create %s certificate: %v", spec.commonName, err)
	}
	privateKey, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal %s key: %v", spec.commonName, err)
	}
	writePEM(t, certPath, "CERTIFICATE", raw)
	writePEM(t, keyPath, "PRIVATE KEY", privateKey)
}

func writePEM(t *testing.T, path, kind string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: data}), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func mustParseURI(t *testing.T, value string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(value)
	if err != nil {
		t.Fatalf("parse URI %q: %v", value, err)
	}
	return parsed
}

func writeManifest(t *testing.T, paths coldPaths, providerDigest, proxyDigest string) manifestMaterial {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate manifest key: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"id": coldManifestID, "schema_version": 1, "capability_version": 1, "signature_version": 1,
		"valid_from": time.Now().Add(-time.Hour).UTC(), "valid_until": time.Now().Add(12 * time.Hour).UTC(),
		"image_digest": providerDigest, "proxy_digest": proxyDigest,
		"routes": []string{"/health", "/v1/chat/completions"},
		"fields": []string{"max_completion_tokens", "messages", "model", "prudentia_request_binding", "stream"},
		"parser": "openai-chat-sse-v1", "identity_profile": 1, "apc_isolation": 1,
	})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	signature := ed25519.Sign(privateKey, payload)
	if err := os.WriteFile(paths.manifestPayload, payload, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := os.WriteFile(paths.manifestSignature, []byte(base64.StdEncoding.EncodeToString(signature)), 0o600); err != nil {
		t.Fatalf("write manifest signature: %v", err)
	}
	pin := sha256.Sum256(payload)
	return manifestMaterial{
		publicKeyBase64: base64.StdEncoding.EncodeToString(publicKey), pin: hex.EncodeToString(pin[:]),
		providerDigest: providerDigest, proxyDigest: proxyDigest,
	}
}

func startColdPostgres(t *testing.T, root string) (*pgxpool.Pool, string) {
	t.Helper()
	migrations, err := filepath.Glob(filepath.Join(root, "migrations", "*.up.sql"))
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	sort.Strings(migrations)
	container, err := tcpostgres.Run(t.Context(), versionValue(t, root, "POSTGRES_IMAGE"),
		tcpostgres.WithDatabase("prudentia"), tcpostgres.WithUsername("prudentia"), tcpostgres.WithPassword("prudentia"),
		tcpostgres.WithOrderedInitScripts(migrations...), tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	connectionString, err := container.ConnectionString(t.Context(), "sslmode=disable")
	if err != nil {
		t.Fatalf("PostgreSQL connection string: %v", err)
	}
	pool, err := pgxpool.New(t.Context(), connectionString)
	if err != nil {
		t.Fatalf("create PostgreSQL pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(t.Context()); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	return pool, connectionString
}

func startInferenceSimulator(t *testing.T, root string) (endpoint, digest string) {
	t.Helper()
	image := versionValue(t, root, "INFERENCE_SIM_IMAGE")
	at := strings.LastIndex(image, "@sha256:")
	if at < 0 {
		t.Fatalf("INFERENCE_SIM_IMAGE must use an immutable sha256 digest: %q", image)
	}
	digest = image[at+1:]
	container, err := testcontainers.Run(t.Context(), image,
		testcontainers.WithExposedPorts("8000/tcp"),
		testcontainers.WithCmd("--model", coldModel, "--mode", "echo", "--seed", "1", "--port", "8000"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/health/ready").WithPort("8000/tcp").WithStartupTimeout(2*time.Minute)),
	)
	if err != nil {
		t.Fatalf("start inference simulator %q: %v", image, err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Errorf("terminate inference simulator: %v", err)
		}
	})
	host, err := container.Host(t.Context())
	if err != nil {
		t.Fatalf("resolve inference simulator host: %v", err)
	}
	port, err := container.MappedPort(t.Context(), "8000/tcp")
	if err != nil {
		t.Fatalf("resolve inference simulator port: %v", err)
	}
	return "http://" + net.JoinHostPort(host, port.Port()), digest
}

func startProcess(t *testing.T, name, binary string, environment map[string]string) *processHandle {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	logs := &boundedLog{limit: 64 << 10}
	command := exec.CommandContext(ctx, binary)
	command.Env = mergedEnvironment(environment)
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		cancel()
		t.Fatalf("start %s: %v", name, err)
	}
	handle := &processHandle{name: name, cancel: cancel, done: make(chan struct{}), logs: logs}
	go func() { handle.finish(command.Wait()) }()
	t.Cleanup(func() {
		handle.cancel()
		select {
		case <-handle.done:
			if err := handle.waitError(); err != nil && !isExpectedProcessExit(err) {
				t.Errorf("%s exited during cleanup: %v\n%s", name, err, handle.logs.String())
			}
		case <-time.After(10 * time.Second):
			t.Errorf("%s did not exit after cancellation", name)
		}
	})
	return handle
}

func isExpectedProcessExit(err error) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr)
}

func mergedEnvironment(overrides map[string]string) []string {
	values := make(map[string]string, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		name, value, ok := strings.Cut(entry, "=")
		if ok {
			values[name] = value
		}
	}
	for name, value := range overrides {
		values[name] = value
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]string, 0, len(names))
	for _, name := range names {
		result = append(result, name+"="+values[name])
	}
	return result
}

func reserveAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release reserved address: %v", err)
	}
	return address
}

func waitForTCP(t *testing.T, name, address string, process *processHandle) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-process.done:
			t.Fatalf("%s exited before readiness: %v\n%s", name, process.waitError(), process.logs.String())
		default:
		}
		connection, err := net.DialTimeout("tcp", address, 250*time.Millisecond)
		if err == nil {
			connection.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s did not become ready\n%s", name, process.logs.String())
}

func waitForHTTP(t *testing.T, name, endpoint string, client *http.Client, process *processHandle) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-process.done:
			t.Fatalf("%s exited before readiness: %v\n%s", name, process.waitError(), process.logs.String())
		default:
		}
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, endpoint, nil)
		if err != nil {
			t.Fatalf("create %s readiness request: %v", name, err)
		}
		response, err := client.Do(request)
		if err == nil {
			io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s did not become ready\n%s", name, process.logs.String())
}

func coldIdentity(t *testing.T) domain.WorkloadIdentity {
	t.Helper()
	identity, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{
		Cluster: coldCluster, Namespace: coldNamespace, LogicalEngine: coldEngine, PodUID: coldPodUID,
		EndpointEpoch: coldEndpointEpoch, RecoveryEpoch: coldRecoveryEpoch,
	})
	if err != nil {
		t.Fatalf("create cold-path identity: %v", err)
	}
	return identity
}

func seedColdBackend(t *testing.T, pool *pgxpool.Pool, proxyEndpoint string, manifest manifestMaterial) {
	t.Helper()
	actor := sha256.Sum256([]byte("cold-inference-integration"))
	tenant := sha256.Sum256([]byte(coldTenant))
	model := sha256.Sum256([]byte(coldModel))
	config := sha256.Sum256([]byte("cold-config"))
	membership := sha256.Sum256([]byte(coldPodUID))
	identity := coldIdentity(t)
	ctx := t.Context()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin cold inference seed: %v", err)
	}
	defer tx.Rollback(ctx)
	exec := func(name, statement string, arguments ...any) {
		t.Helper()
		if _, err := tx.Exec(ctx, statement, arguments...); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	exec("admission state", `INSERT INTO system_admission_state(
		cluster_id,recovery_epoch,admission_state,dispatch_state,schema_write_version,
		lookup_write_version,digest_write_version,capability_kek_write_version,
		capability_comparison_write_version,classification_policy_version,
		cleanup_policy_version,changed_by_hash)
		VALUES($1,$2,'open','open',9,1,1,1,1,1,1,$3)
		ON CONFLICT(cluster_id) DO UPDATE SET
			recovery_epoch=EXCLUDED.recovery_epoch,admission_state='open',dispatch_state='open',
			schema_write_version=9,lookup_write_version=1,digest_write_version=1,
			capability_kek_write_version=1,capability_comparison_write_version=1,
			fenced_at=NULL,fenced_by_hash=NULL,fence_reason=NULL,reopened_at=transaction_timestamp(),
			changed_at=transaction_timestamp(),changed_by_hash=EXCLUDED.changed_by_hash`,
		identity.Cluster(), identity.RecoveryEpoch(), actor[:])
	exec("lookup versions", `INSERT INTO system_lookup_read_versions(cluster_id,version) VALUES($1,1) ON CONFLICT DO NOTHING`, identity.Cluster())
	exec("digest versions", `INSERT INTO system_digest_read_versions(cluster_id,version) VALUES($1,1) ON CONFLICT DO NOTHING`, identity.Cluster())
	exec("capability manifest", `INSERT INTO capability_manifests(
		manifest_id,manifest_version,image_digest,proxy_digest,supported_routes,supported_fields,
		response_parsers,identity_profile,apc_isolation_mode,termination_capabilities,
		signature_algorithm,signature_key_version,signature,valid_from,valid_until)
		VALUES($1,1,$2,$3,'["/health","/v1/chat/completions"]','{"messages":true,"model":true,"stream":true}',
		'["openai-chat-sse-v1"]','{"mode":"exact_workload_mtls"}','disabled','{}',
		'ed25519',1,decode('01','hex'),transaction_timestamp()-interval '1 hour',transaction_timestamp()+interval '12 hours')`,
		coldManifestID, manifest.providerDigest, manifest.proxyDigest)
	exec("tenant counter", `INSERT INTO tenant_counters(tenant_hash,grant_limit) VALUES($1,4)`, tenant[:])
	exec("capacity", `INSERT INTO instance_capacity(
		cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,
		physical_slots,admission_limit,projection_version)
		VALUES($1,$2,$3,$4,$5,$6,2,2,1)`,
		identity.Cluster(), identity.Namespace(), identity.LogicalEngine(), identity.PodUID(),
		identity.EndpointEpoch(), identity.RecoveryEpoch())
	exec("observations", `INSERT INTO source_observations(
		cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,
		source_kind,writer_generation,source_sequence,accepted_at,expires_at,
		ttl_policy_version,schema_version,normalized_payload)
		VALUES
		($1,$2,$3,$4,$5::bigint,$6::bigint,'structural',1,1,transaction_timestamp(),transaction_timestamp()+interval '1 hour',1,1,
		 jsonb_build_object('endpoint',$7::text,'model',$8::text,'workload_uid','cold-workload','endpoint_epoch',$5::bigint,'recovery_epoch',$6::bigint)),
		($1,$2,$3,$4,$5::bigint,$6::bigint,'runtime_health',1,1,transaction_timestamp(),transaction_timestamp()+interval '1 hour',1,1,
		 '{"state":2,"warm":true}')`,
		identity.Cluster(), identity.Namespace(), identity.LogicalEngine(), identity.PodUID(),
		identity.EndpointEpoch(), identity.RecoveryEpoch(), proxyEndpoint, coldModel)
	exec("projection", `INSERT INTO instance_projections(
		cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,
		normalized_proxy_endpoint,model_fingerprint,config_fingerprint,membership_fingerprint,
		capability_manifest_id,capability_manifest_version,source_stamps,health,projection_version)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,1,'{}','healthy',1)`,
		identity.Cluster(), identity.Namespace(), identity.LogicalEngine(), identity.PodUID(),
		identity.EndpointEpoch(), identity.RecoveryEpoch(), proxyEndpoint, model[:], config[:], membership[:], coldManifestID)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit cold inference seed: %v", err)
	}
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

func requireNoProcessExit(t *testing.T, handles ...*processHandle) {
	t.Helper()
	for _, handle := range handles {
		select {
		case <-handle.done:
			t.Fatalf("%s exited unexpectedly: %v\n%s", handle.name, handle.waitError(), handle.logs.String())
		default:
		}
	}
}

func coldLedgerState(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	rows, err := pool.Query(t.Context(), `SELECT r.state,q.stage,coalesce(q.outcome,''),r.slot_cost
		FROM reservations r JOIN request_records q USING(request_id) ORDER BY r.created_at`)
	if err != nil {
		return "query error: " + err.Error()
	}
	defer rows.Close()
	var states []string
	for rows.Next() {
		var reservation, stage, outcome string
		var slotCost int
		if err := rows.Scan(&reservation, &stage, &outcome, &slotCost); err != nil {
			return "scan error: " + err.Error()
		}
		states = append(states, fmt.Sprintf("reservation=%s stage=%s outcome=%s cost=%d", reservation, stage, outcome, slotCost))
	}
	var active, orphaned, limit int
	tenant := sha256.Sum256([]byte(coldTenant))
	if err := pool.QueryRow(t.Context(), `SELECT active_grants,orphaned_grants,grant_limit FROM tenant_counters WHERE tenant_hash=$1`, tenant[:]).Scan(&active, &orphaned, &limit); err != nil {
		states = append(states, "tenant query error: "+err.Error())
	} else {
		states = append(states, fmt.Sprintf("tenant active=%d orphaned=%d limit=%d", active, orphaned, limit))
	}
	return strings.Join(states, "; ")
}

func assertColdLedgerReleased(t *testing.T, pool *pgxpool.Pool, wantTerminal int) {
	t.Helper()
	var released, activeDebt, reserved, orphaned, admission int
	var retired bool
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM reservations WHERE state='released'`).Scan(&released); err != nil {
		t.Fatalf("read released reservations: %v", err)
	}
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM orphaned_capacity_debts WHERE state='active'`).Scan(&activeDebt); err != nil {
		t.Fatalf("read active debt: %v", err)
	}
	if err := pool.QueryRow(t.Context(), `SELECT reserved_slots,orphaned_slots,admission_limit,retired FROM instance_capacity WHERE cluster_id=$1 AND pod_uid=$2`, coldCluster, coldPodUID).Scan(&reserved, &orphaned, &admission, &retired); err != nil {
		t.Fatalf("read cold-path capacity: %v", err)
	}
	if released != wantTerminal || activeDebt != 0 || reserved != 0 || orphaned != 0 || admission != 2 || retired {
		t.Fatalf("ledger not released: released=%d want=%d active_debt=%d reserved=%d orphaned=%d admission=%d retired=%t", released, wantTerminal, activeDebt, reserved, orphaned, admission, retired)
	}
}

func addressURL(scheme, address string) string {
	return fmt.Sprintf("%s://%s", scheme, address)
}
