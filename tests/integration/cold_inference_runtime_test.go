//go:build integration

package integration_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

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
	ctx, cancel := context.WithCancel(t.Context())
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
