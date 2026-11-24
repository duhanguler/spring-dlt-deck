# Changelog

All notable changes to **`spring-dlt-deck`** will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-24

### Added
- **Pure Java 25 Core Engine (`dlt-deck-core`):** Final `ScopedValue<ActorContext>` (JEP 506), `Stream.gather` (JEP 485) sliding-window `BlastRadiusGuard`, Flexible Constructor Bodies (JEP 513), deterministic SHA-256 `ExceptionFingerprinter`, Confluent `0x00` `NOT_PATCHABLE` & Tombstone & CloudEvents `PayloadCodec`, checksum-verified `PiiRedactor` (TCKN, IBAN, Luhn PAN, JWT, email, phone), OWASP `CsvExportSanitizer`, bounded RFC 7396 `JsonPayloadPatcher`, canonical HMAC-SHA256 `KeysetCursorCodec`, `MakerCheckerPolicy` (`planSpecHash` TOCTOU defense), SHA-256 `AuditHashChain` + batch Merkle roots, custom JFR events, and hexagonal SPIs.
- **3-Table Hot/Cold/Deduplicated JDBC Storage (`dlt-deck-storage-jdbc`):** Isolated advisory-locked `DltDeckSchemaMigrator` (`dlt_deck_schema_history`), Spring `JdbcClient` + `batchUpdate()` repositories, GZIP compression + `PayloadBlobStore` SPI, `FOR UPDATE SKIP LOCKED` stripe leasing, batched TTL purge, PostgreSQL 17 & H2 support.
- **Optional Distributed Redis Limiter (`dlt-deck-storage-redis`):** Atomic Lua token-bucket script for cluster-wide multi-pod replay rate limiting.
- **Isolated Spring Boot 4.2 Starter (`dlt-deck-spring-boot-starter`):** Zero global `WebMvcConfigurer`, isolated WebMvc.fn `RouterFunction`, private Jackson 3 `JsonMapper`, `@Qualifier("dltDeckInternal")` Kafka 4.4 KIP-848 ingestion & key-striped virtual-thread replay executor (two short TXs + fencing token + `INDETERMINATE` + per-key HoL barrier), `DltDeckIdempotentReplayFilter`, Honest 3-Tier Dry-Run, JEP 290/415 `ObjectInputFilter`, fail-closed `SecurityFilterChain` + CSRF + Strict Nonce CSP + `Cache-Control: no-store`, zero-Node Precision Workbench UI (Carbon Dark & Swiss Light), Actuator `/actuator/dltdeck`, Micrometer/HealthIndicator, and Spring AOT `RuntimeHints`.
- **Fail-Closed HITL MCP Server (`dlt-deck-mcp`):** ArchUnit-verified read-only triage, Tier-1 dry-run, and `PENDING_APPROVAL` plan drafting only.
- **CI Test Guardrails (`dlt-deck-test-support`), JMH Benchmarks (`dlt-deck-benchmarks`), E-Commerce Demo (`dlt-deck-demo`), & Helm Chart (`deploy/helm/spring-dlt-deck`).**
