# ADR-003: 3-Table Hot/Cold/Deduplicated PostgreSQL Schema & Batch Merkle-Root Audit Checkpoints

- **Status:** Accepted
- **Date:** 2026-11-24
- **Context:** Surviving 500,000+ message DLT incident storms without PostgreSQL TOAST bloat or global audit lock serialization bottlenecks.

## Problem Statement

1. **TOAST & Autovacuum Explosion:** Storing an 8 KB Java stacktrace and a 20 KB JSON payload inline in a single DLT table turns a 50,000-message incident of the same bug into **400 MB of duplicated stacktraces** and **1 GB of TOAST chunks** scanned on every triage query and updated on every status transition.
2. **Global Audit Chain Lock Contention:** Appending a hash-chained audit row for *every* individual replayed message forces global serial locking across all replay threads, capping cluster throughput to single-row DB commit latency.
3. **Timestamp Precision Drift:** Java `Instant` carries nanosecond precision on Linux, while PostgreSQL `TIMESTAMPTZ` stores microseconds. Hashing raw nanoseconds breaks audit verification on readback.

## Decision

1. **3-Table Normalized Storage:**
   - `dlt_deck_fingerprints`: Stores the canonical stacktrace, normalized root cause, and remediation advice **once per SHA-256 fingerprint cluster** (50k duplicates = 8 KB instead of 400 MB).
   - `dlt_deck_messages`: Narrow, cache-hot triage & replay index with partial indexes for keyset pagination, stripe ordering, and lease expiration. Zero large `TEXT`/`BYTEA` blobs.
   - `dlt_deck_payloads`: Cold store supporting bounded `maxPayloadBytes`, automatic GZIP compression above threshold, pluggable `PayloadBlobStore` SPI (for S3/MinIO spill), and batched TTL retention purges (`DELETE ... LIMIT 1000`).
2. **Batch SHA-256 Merkle-Root Audit Checkpoints:**
   - Control-plane governance mutations (`DRAFT_REPLAY_PLAN`, `APPROVE_REPLAY_PLAN`, `UNMASK_PII`, `RESOLVE_MANUAL_STATE`) append directly to `dlt_deck_audit_ledger` under `pg_advisory_xact_lock`.
   - High-throughput data-plane replays commit a **SHA-256 Merkle root checkpoint** over every `N` replayed message IDs (default 50), delivering cryptographic tamper evidence with 50x lower lock contention.
   - All audited timestamps are truncated to `ChronoUnit.MICROS` prior to hashing and persistence for bit-exact verification across PostgreSQL 17 and H2.
