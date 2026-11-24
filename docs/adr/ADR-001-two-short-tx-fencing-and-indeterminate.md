# ADR-001: Two Short Transactions + Fencing Token + `INDETERMINATE` State Instead of Holding `SKIP LOCKED` Across Kafka I/O

- **Status:** Accepted
- **Date:** 2026-11-24
- **Context:** Multi-pod Dead-Letter Topic (DLT) replay control plane backed by PostgreSQL 17 and Apache Kafka 4.4.

## Problem Statement

Naive Kafka DLT replay tools commit two classic distributed-systems mistakes:

1. **Database Connection Starvation:** Opening a PostgreSQL transaction with `SELECT ... FOR UPDATE SKIP LOCKED`, calling `kafkaTemplate.send(...).get()` inside the open transaction, and then issuing `UPDATE ... SET status = 'PUBLISHED'`. During a Kafka broker leader election or network slowdown (5–30 s), every replay worker pins a HikariCP connection, exhausting the host application's database pool and triggering cascading outages.
2. **Dishonest "Exactly-Once" Claims & Silent Duplicate Replay:** Claiming that setting Kafka producer `enable.idempotence=true` provides cross-restart exactly-once replay is **false** — idempotent producer PIDs reset across JVM restarts and failovers. If a pod commits a Kafka publish and crashes *before* marking the DB row `PUBLISHED`, a naive lease sweeper that resets expired `CLAIMED` rows back to `NEW` will silently publish the message a **second time** (e.g., double-charging a payment).

## Decision

`spring-dlt-deck` enforces a **Two Short Transactions + Monotonic Fencing Token + Honest `INDETERMINATE` State** protocol:

1. **TX1 (Short Claim, <5 ms):** Atomically transitions `NEW | FAILED_PUBLISH -> CLAIMED`, increments `fencing_token = fencing_token + 1`, sets `lease_owner` and `lease_expires_at`, and **commits immediately**. Zero database connections are held during Kafka I/O.
2. **Kafka Publish (Zero DB Lock):** Publishes to the validated target topic with `acks=all`, `enable.idempotence=true`, sanitized headers, and a deterministic `x-dlt-deck-idempotency-key = SHA-256(planId | dltTopic | dltPartition | dltOffset)`.
3. **TX2 (Short Fenced Commit, <5 ms):**
   - On broker ACK: `UPDATE dlt_deck_messages SET status = 'PUBLISHED', published_partition = ?, published_offset = ? WHERE id = ? AND status = 'CLAIMED' AND fencing_token = ?`.
   - On synchronous/broker failure (known not committed): `UPDATE ... SET status = 'FAILED_PUBLISH' WHERE id = ? AND fencing_token = ?`.
4. **Honest Crash Recovery (`INDETERMINATE`):** If a pod crashes after Kafka ACK and before TX2, the background lease sweeper transitions expired `CLAIMED` rows **strictly to `INDETERMINATE` — never back to `NEW`**. An `APPROVER` must explicitly inspect and resolve `INDETERMINATE` records (to `PUBLISHED`, `NEW`, or `DISCARDED`) with a tamper-evident audit entry.
5. **End-to-End Effective Exactly-Once:** Host consumers can optionally plug in `DltDeckIdempotentReplayFilter` to deduplicate on `x-dlt-deck-idempotency-key`, making even manual `INDETERMINATE -> NEW` replays safe end-to-end.

## Consequences

- **Positive:** Zero HikariCP pool exhaustion during Kafka latency spikes; zero silent double-replays after mid-publish pod crashes; crystal-clear operator visibility into ambiguous crashes.
- **Trade-off:** Requires explicit `APPROVER` triage when a pod crashes in the sub-millisecond window between Kafka ACK and TX2 — an intentional fail-closed safety trade-off over silent corruption.
