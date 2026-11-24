# ADR-002: Deterministic Stripe Leasing + Per-Key Head-of-Line Barrier for Multi-Pod Key-Ordered Replay

- **Status:** Accepted
- **Date:** 2026-11-24
- **Context:** Horizontally scaled Kubernetes deployments (`replicas >= 2`) executing `PRESERVE_KEY_ORDER` replay plans.

## Problem Statement

Running `SELECT ... FOR UPDATE SKIP LOCKED` across multiple pods on individual message rows breaks per-key ordering: Pod A claims `order-42:Created` (offset 10) while Pod B simultaneously claims `order-42:Captured` (offset 11) and publishes `Captured` before `Created`. Worse, if `order-42:Created` fails to publish and Pod A continues processing `order-42:Authorized` and `order-42:Captured`, downstream state machines corrupt permanently.

## Decision

1. **Deterministic Stripe Partitioning (`dlt_deck_plan_stripes`):**
   - Each message deterministically maps to stripe `S = floorMod(COALESCE(original_partition, message_key_hash), stripeCount)`.
   - All messages sharing the same `original_partition` or `message_key` always land in the **exact same stripe `S`**.
   - Pods acquire renewable stripe leases via `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1` on `dlt_deck_plan_stripes` with monotonic `fencing_token`. At most **one pod** executes stripe `S` at any instant, processing records in strict `(COALESCE(original_offset, dlt_offset) ASC, dlt_offset ASC)` order.
2. **Per-Key Head-of-Line (HoL) Barrier:**
   - Every message stores `key_fingerprint = SHA-256(message_key)`.
   - In `PRESERVE_KEY_ORDER` mode, if message `#i` for key `K` fails (`FAILED_PUBLISH`) or hits a mid-publish crash (`INDETERMINATE`), the executor immediately marks all subsequent eligible records for `(dlt_topic, key_fingerprint)` as **`BLOCKED_BY_PRIOR_KEY_FAILURE`** without stalling unrelated keys in the same or other stripes.

## Consequences

- **Positive:** Horizontal replay parallelism across stripes while preserving strict per-key FIFO ordering and isolating bad keys without global head-of-line blocking.
- **Verification:** Proven under live multi-pod concurrency by `MultiPodKeyOrderedReplayAndHoLBarrierIT` against PostgreSQL 17 + Apache Kafka 4 KRaft.
