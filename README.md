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

No protobuf generation command or integration-test dependency is added until
the first protobuf or PostgreSQL-backed runnable slice needs it.

## Gateway prototype

The current runnable slice implements the public gateway boundary: static
API-key authentication, per-tenant model authorization, bounded strict JSON
decoding, synchronous SSE and bounded nonstreaming response sinks, sanitized
errors, request correlation IDs, health probes, and graceful HTTP shutdown.

Configure and run it with:

```text
PRUDENTIA_GATEWAY_API_KEY=<at-least-16-byte-secret>
PRUDENTIA_GATEWAY_TENANT=<tenant>
PRUDENTIA_GATEWAY_MODELS=<comma-separated-model-allowlist>
PRUDENTIA_GATEWAY_LISTEN=127.0.0.1:8080
go run ./cmd/gateway
```

`PRUDENTIA_GATEWAY_LISTEN` defaults to `127.0.0.1:8080`; every other variable
is required. `/livez` and `/startupz` report healthy after the listener starts.
`/readyz` deliberately reports `503`, and authenticated
`POST /v1/chat/completions` requests deliberately return a sanitized `503`,
until the PostgreSQL-backed scheduler and exact pod-identity provider path are
composed. The prototype does not add an unsafe direct-to-vLLM fallback.