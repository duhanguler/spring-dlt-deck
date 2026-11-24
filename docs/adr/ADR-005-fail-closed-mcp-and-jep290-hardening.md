# ADR-005: Fail-Closed HITL MCP Governance & JEP 290/415 `ObjectInputFilter` Hardening

- **Status:** Accepted
- **Date:** 2026-11-24
- **Context:** Safe AI-assisted incident triage via Model Context Protocol (MCP) and untrusted poison-pill deserialization from `ErrorHandlingDeserializer`.

## Decision

1. **Compile-Time & ArchUnit-Proven Fail-Closed MCP (`dlt-deck-mcp`):**
   - Exposes strictly 4 tools under `ActorRole.MCP_AGENT` (`ScopedValue<ActorContext>`): `list_dlt_clusters`, `inspect_dlt_message` (hard-coded `unmaskPii = false`), `dry_run_dlt_patch` (hard-coded `TIER_1_VALIDATE` only), and `draft_replay_plan` (forced `requiresApproval = true`, `PENDING_APPROVAL`).
   - `McpSecurityArchitectureTest` (`archunit-junit6`) fails the build if any class in `dlt-deck-mcp` references `approveReplayPlan`, `rejectReplayPlan`, `executeApprovedPlan`, `resolveIndeterminate`, or `discardMessage`.
2. **JEP 290/415 `ObjectInputFilter` for `ErrorHandlingDeserializer`:**
   - Spring Kafka stores `DeserializationException` as Java serialized bytes in `springDeserializerExceptionValue`. Calling unfiltered `ObjectInputStream.readObject()` on attacker-reachable Kafka headers opens a critical RCE gadget vector.
   - `SafeSpringHeaderExtractor` enforces a strict allowlist (`DeserializationException`, `KafkaException`, `NestedRuntimeException`, `RecordHeaders`, `RecordHeader`, `java.lang.*`, `!*`) with `maxdepth=12;maxrefs=256;maxbytes=524288`. `StarterArchitectureTest` proves no other class in the starter may touch `ObjectInputStream`.
3. **TOCTOU & Browser Security Defense:**
   - Replay plans freeze `planSpecHash` (SHA-256 of canonical parameters + `snapshotCount`) before approval; any mismatch at approval/execution returns `409 Conflict` RFC 9457 `ProblemDetail`.
   - Workbench enforces per-request 128-bit cryptographic Nonce CSP (zero `'unsafe-inline'`, zero external CDN), `X-XSRF-TOKEN` CSRF protection, `Cache-Control: no-store`, HMAC-SHA256 canonical Base64url keyset cursors, and OWASP CSV formula neutralization.
