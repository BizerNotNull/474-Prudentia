# AGENTS.md

## Scope and authority

This file applies to the entire repository. `docs/architecture.md` is the current architecture contract and source of truth for system boundaries, invariants, repository layout, and acceptance gates. Read the sections relevant to a change before editing code. If implementation and architecture disagree, do not silently weaken an invariant: update both deliberately or raise the conflict.

The repository is currently an architecture-first Go scaffold. Do not invent commands, generated artifacts, deployment assumptions, or compatibility guarantees that are not yet represented in the repository.

## Project

Prudentia is a single-cluster inference control layer for Kubernetes-hosted vLLM. It has three continuously running binaries:

- `cmd/gateway`: public HTTP ingress and synchronous response owner.
- `cmd/scheduler`: internal scheduling gRPC service and separately addressed privileged admin service.
- `cmd/controller`: Kubernetes observer and level reconciler.

PostgreSQL is the authoritative transactional ledger. Kubernetes observations, process memory, telemetry, and provider metrics are not transactional authority.

### Non-negotiable design rules

- Keep public and domain contracts provider-neutral. vLLM routes, DTOs, SSE parsing, metrics, and diagnostics belong only in `internal/adapter/vllm`.
- Only `internal/adapter/kubernetes` may import Kubernetes API objects.
- `internal/domain` must not import transport, database, Kubernetes, or provider packages.
- Put inward-facing interfaces beside the package that consumes them; do not create a generic `ports` package.
- Ranking is pure and deterministic: no I/O, mutation, clock reads, or randomness. Reservation and all authoritative rechecks happen transactionally in PostgreSQL.
- Capacity is reserved before dispatch. A timeout, cancellation, socket close, bare EOF, or elapsed time never proves provider execution stopped and must not restore possibly dispatched capacity.
- Never automatically replay a provider POST after execution may have begun.
- Preserve the single synchronous response owner; do not add detached stream goroutines or unbounded channels.
- Verify the exact Pod UID and endpoint epoch over mTLS before exposing or sending request-body bytes.
- Use database time for authoritative freshness and classification decisions.
- Watches enqueue keys; reconcilers read current level state. Do not treat watch events as commands.
- Raw prompts, completions, idempotency keys, credentials, provider bodies, plaintext capabilities, and mover tickets must not be persisted, logged, traced, or used as metric labels.
- Optional provider capabilities fail closed and require an exact, pinned, signed capability manifest plus contract coverage.

## Intended repository layout

Follow the layout in `docs/architecture.md`:

- `api/`: protobuf source definitions.
- `cmd/`: composition roots only; keep business logic in `internal/`.
- `internal/domain`: immutable backend-neutral values and errors.
- `internal/{request,admission,scheduling,registry,controller,cache}`: application and policy packages.
- `internal/transport`: public HTTP and gRPC boundaries.
- `internal/adapter`: PostgreSQL, Kubernetes, scheduler-client, vLLM, and optional KV-mover adapters.
- `migrations/`: ordered PostgreSQL migrations.
- `deploy/`: Kubernetes, identity, policy, and workload artifacts.
- `tests/{contract,functional,integration}`: tests that span package or process boundaries.

Do not create a second layout or move boundary-specific DTOs into shared/domain packages for convenience.

## Go development

- Use the Go version declared in `go.mod` (currently Go 1.26.1).
- Format changed Go files with `gofmt`.
- Keep packages focused and dependency direction inward. Avoid package globals, hidden initialization, and ambient clocks/randomness.
- Pass `context.Context` explicitly across I/O boundaries; honor cancellation without treating it as positive execution-stop evidence.
- Wrap errors with operation context and preserve causes. Convert internal/provider errors to stable public or typed gRPC errors only at transport boundaries.
- Prefer validated constructors and immutable value objects. Defensively copy mutable input and output; use explicit `(value, bool)` optional accessors rather than exported mutable pointers.
- Bound request bodies, responses, queues, retries, worker pools, deadlines, diagnostics, and shutdown waits.
- Keep configuration explicit, validated at startup, and fail closed on unknown enum/schema/capability versions.
- Avoid logging in pure domain and policy code. Use structured, low-cardinality telemetry at application and adapter boundaries.
- Do not hand-edit generated protobuf or Kubernetes output. Change its source and use the repository generator once one is added.
- PostgreSQL migrations are append-only after merge. Preserve documented lock ordering and make state transitions idempotent under retries.

## Testing and verification

For permanent behavior changes, add the narrowest test that proves the observable contract and then run the affected package tests. Before opening a PR, once Go packages exist, run:

```text
gofmt -w <changed .go files>
go test ./...
go vet ./...
```

At the current scaffold stage, `go test ./...` reports that no packages exist; documentation-only changes should record that tests are not applicable rather than claiming a passing suite.

Test behavior and invariants, not SQL text, log wording, private helper structure, sleeps, or incidental ordering.

- Unit tests: in-process, deterministic, no sockets, SQL, Kubernetes, or provider.
- Functional tests: package/process boundaries with deterministic doubles; use production-compatible ephemeral PostgreSQL when SQL semantics matter.
- Integration tests: real process/network topology and external systems. Use `llm-d-inference-sim` as the routine vLLM mock.
- Provider conformance tests: retain separately pinned real-vLLM coverage for exact routes, fields, SSE behavior, metrics, tokenizer/provider behavior, and capability manifests.

### `llm-d-inference-sim`

- Pin the simulator image by immutable version or digest; never use `latest`.
- Prefer a dummy model and deterministic configuration (for example, echo mode and fixed latency) so tests require neither GPUs nor model downloads.
- Start it through the integration harness, wait on a readiness endpoint, isolate each test's configuration, and always clean it up.
- Exercise both streaming and bounded nonstreaming responses plus deliberate failure/latency cases used by the changed path.
- Treat it as a protocol and control-plane test double, not proof of model correctness, real vLLM performance, batching, GPU behavior, KV-cache semantics, or complete provider compatibility.
- When exact Pod identity is under test, place the simulator behind the same authenticated per-Pod proxy boundary used by deployment; the simulator itself is not proof of Pod UID/endpoint-epoch identity.

Concurrency, drain/reserve races, pre-dispatch give-up, ambiguous dispatch debt, controller handoff fencing, identity replacement, schema/key rotation, and recovery tests must assert the invariants listed in `docs/architecture.md` §8. Do not “fix” flakes in these areas with sleeps; use controllable clocks, barriers, and explicit event synchronization.

## Change discipline

- Keep changes focused. Remove obsolete paths when replacing an API; do not leave compatibility aliases unless the architecture explicitly requires a rolling-version bridge.
- Update `docs/architecture.md` when changing a documented invariant, boundary, external contract, repository layout, or acceptance gate.
- Update user/operator documentation and deployment examples for visible behavior, configuration, compatibility, or rollout changes.
- Security-sensitive changes require negative tests for authentication, authorization, redaction, identity mismatch, unknown versions, and fail-closed behavior as applicable.
- Never weaken safety to improve availability without an explicit architecture decision.

## Git and pull requests

- Branch names use `<type>/<description>`, for example `feat/weighted-routing`.
- Use Conventional Commits: `<type>(<scope>): <summary>`. Use a meaningful subsystem scope or `repo` for repository-wide changes.
- PR titles use the same `<type>(<scope>): <summary>` form.
- Keep PRs small and single-purpose. Complete `.github/PULL_REQUEST_TEMPLATE.md`, link related issues, and list exact validation commands and relevant manual checks.
- Include tests for changed behavior, or state precisely why tests are not applicable.
- Call out breaking changes, compatibility effects, rollout steps, migration ordering, and recovery implications.
- Do not merge until required checks and reviews pass. Use squash merging.
