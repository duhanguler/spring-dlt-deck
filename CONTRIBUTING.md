# Contributing to `spring-dlt-deck`

Thank you for your interest in contributing to **`spring-dlt-deck`**!

## Prerequisites

- **Java 25 LTS** (no `--enable-preview` flags allowed)
- **Docker** (for Testcontainers 2.0.5 PostgreSQL 17, Apache Kafka 4 KRaft, and Redis 7 integration tests)
- **Maven Wrapper:** Use `./mvnw` (pinned to Maven `3.9.16`)

## Local Verification Gate

Before opening a pull request, run the full verification suite:

```bash
./mvnw spotless:apply
./mvnw clean verify
```

Every pull request must satisfy:
1. **Zero ErrorProne & NullAway errors** (`@NullMarked` JSpecify mode across all modules).
2. **ArchUnit invariants** (`dlt-deck-core` free of Spring/JDBC/Kafka; zero `ThreadLocal`; zero global `WebMvcConfigurer` in starter; fail-closed `dlt-deck-mcp` boundaries).
3. **JaCoCo coverage** (≥85% line / ≥80% branch on `dlt-deck-core`).
4. **Property-based fuzzing (`jqwik`)** and deterministic `java.time.Clock`-based unit tests (no flaky `Thread.sleep` assertions in unit tests).
5. **All Testcontainers 2.0.5 ITs** passing cleanly.

## Submitting Changes

1. Fork the repository and create a feature branch (`git checkout -b feat/my-improvement`).
2. Add or update unit/integration tests covering your changes.
3. For architectural changes, include or update an ADR in `docs/adr/`.
4. Open a Pull Request describing the motivation, invariants preserved, and verification output.
