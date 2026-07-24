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