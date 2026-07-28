---
name: use-local-postgres-17-for-tests
description: "Use the locally available `postgres:17` image for PostgreSQL tests instead of pulling another tag"
condition: "postgres:(?:17\\.10-bookworm|18\\.4-trixie)"
scope: "tool:hub"
---

For local PostgreSQL test or migration containers, use the already available `postgres:17` image. Do not start or pull `postgres:17.10-bookworm`, `postgres:18.4-trixie`, or another remote tag merely to run tests. This local testing choice is independent of the repository's documented target image.