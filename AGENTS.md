# AGENTS.md

## Project

Prudentia is a Go control plane deployed between clients and multiple vLLM/SGLang inference instances. Keep routing, scheduling, health checking, and provider-specific integration separated; avoid leaking backend details into client-facing APIs.

## Development

## Git

- Branch names use `<type>/<description>`, for example `feat/weighted-routing`.
- Use Conventional Commits:
- PR titles use `<type>(<scope>): <summary>`; use a meaningful subsystem or `repo` for repository-wide changes.
- Keep PRs small and single-purpose. Complete the PR template, link related issues, and include tests or explain why none are needed.
- Update documentation for user-visible behavior and call out breaking changes, rollout concerns, and compatibility effects.
- Do not merge until required checks and reviews pass.
- Use squash merging.
