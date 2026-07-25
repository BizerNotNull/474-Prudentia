# 474-Prudentia

## Development baseline

Pinned development and test versions live in `.github/versions.env`:

- PostgreSQL tests use the official `postgres:17.10-bookworm` image.
- Protobuf generation uses `protoc` 35.1, `protoc-gen-go` v1.36.11, and
  `protoc-gen-go-grpc` v1.6.2.
- Database migrations use `golang-migrate` v4.19.1 and ordered SQL files in
  `migrations/`.
- PostgreSQL integration tests use Testcontainers for Go v0.43.0 to start the
  pinned image through the Go test harness. Tests must wait for readiness and
  register container cleanup with `testing.T`; Docker Compose is not part of
  the test path.

CI rejects unformatted Go files, then runs `go test ./...` and `go vet ./...`.

Regenerate the scheduler protobuf bindings with:

```text
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/scheduler/v1/scheduler.proto
```

## Runnable gateway and scheduler slice

The runnable request path now includes:

- the existing authenticated OpenAI-compatible gateway;
- a mutually authenticated gRPC scheduler client and server;
- deterministic capacity ranking and PostgreSQL-transactional reservation;
- encrypted recoverable reservation capabilities;
- dispatch authorization, terminal release, pre-dispatch give-up, ambiguous
  dispatch debt, and database-time expiry classification;
- an exact-workload-identity HTTPS adapter for vLLM streaming responses.

Apply the ledger schema before starting the scheduler:

```text
psql "$PRUDENTIA_DATABASE_URL" -f migrations/000001_scheduler_mvp.up.sql
```

The scheduler intentionally does not invent capacity. A healthy, unexpired
`scheduler_backends` row must be written by the controller/catalog path before
it can reserve work. There is no static direct-to-vLLM fallback.

Scheduler configuration:

```text
PRUDENTIA_DATABASE_URL=<postgres-connection-url>
PRUDENTIA_SCHEDULER_CAPABILITY_KEY=<base64-encoded-32-byte-key>
PRUDENTIA_SCHEDULER_LISTEN=127.0.0.1:9090
PRUDENTIA_SCHEDULER_TLS_CERT=<scheduler-certificate.pem>
PRUDENTIA_SCHEDULER_TLS_KEY=<scheduler-private-key.pem>
PRUDENTIA_SCHEDULER_CLIENT_CA=<gateway-client-ca.pem>
PRUDENTIA_GATEWAY_SPIFFE_ID=<exact-gateway-spiffe-uri>
go run ./cmd/scheduler
```

Gateway configuration:

```text
PRUDENTIA_GATEWAY_API_KEY=<at-least-16-byte-secret>
PRUDENTIA_GATEWAY_TENANT=<tenant>
PRUDENTIA_GATEWAY_MODELS=<comma-separated-model-allowlist>
PRUDENTIA_GATEWAY_LISTEN=127.0.0.1:8080
PRUDENTIA_SCHEDULER_ADDRESS=127.0.0.1:9090
PRUDENTIA_SCHEDULER_SERVER_NAME=<scheduler-certificate-dns-name>
PRUDENTIA_SCHEDULER_CA=<scheduler-server-ca.pem>
PRUDENTIA_GATEWAY_TLS_CERT=<gateway-client-certificate.pem>
PRUDENTIA_GATEWAY_TLS_KEY=<gateway-client-private-key.pem>
PRUDENTIA_PROVIDER_CA=<provider-proxy-ca.pem>
PRUDENTIA_PROVIDER_TRUST_DOMAIN=<provider-spiffe-trust-domain>
go run ./cmd/gateway
```

The gateway fails startup unless the scheduler health service is reachable over
mTLS. `/readyz` reports ready only after that startup check. Before sending any
request body, the provider adapter verifies the exact SPIFFE URI derived from
the reserved cluster, namespace, logical engine, Pod UID, endpoint epoch, and
recovery epoch. Bare EOF, cancellation, or a transport failure after body
consumption is never treated as positive execution-stop evidence.