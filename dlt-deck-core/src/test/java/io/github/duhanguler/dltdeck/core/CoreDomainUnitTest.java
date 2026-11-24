package io.github.duhanguler.dltdeck.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.duhanguler.dltdeck.core.codec.DefaultPayloadCodec;
import io.github.duhanguler.dltdeck.core.codec.PayloadCodec;
import io.github.duhanguler.dltdeck.core.context.ActorContext;
import io.github.duhanguler.dltdeck.core.context.ActorRole;
import io.github.duhanguler.dltdeck.core.exception.ForbiddenActionException;
import io.github.duhanguler.dltdeck.core.exception.NotFoundException;
import io.github.duhanguler.dltdeck.core.exception.PlanConflictException;
import io.github.duhanguler.dltdeck.core.exception.RateLimitExceededException;
import io.github.duhanguler.dltdeck.core.exception.ValidationDomainException;
import io.github.duhanguler.dltdeck.core.fingerprint.ExceptionFingerprinter;
import io.github.duhanguler.dltdeck.core.jfr.DltBlastRadiusTripEvent;
import io.github.duhanguler.dltdeck.core.jfr.DltIndeterminateRecoveryEvent;
import io.github.duhanguler.dltdeck.core.jfr.DltIngestionBatchEvent;
import io.github.duhanguler.dltdeck.core.jfr.DltReplayRecordEvent;
import io.github.duhanguler.dltdeck.core.masking.CsvExportSanitizer;
import io.github.duhanguler.dltdeck.core.masking.PiiRedactor;
import io.github.duhanguler.dltdeck.core.model.AuditChainVerification;
import io.github.duhanguler.dltdeck.core.model.AuditEntry;
import io.github.duhanguler.dltdeck.core.model.BatchMerkleCheckpoint;
import io.github.duhanguler.dltdeck.core.model.DltMessageDetail;
import io.github.duhanguler.dltdeck.core.model.DltMessageSummary;
import io.github.duhanguler.dltdeck.core.model.DryRunOutcome;
import io.github.duhanguler.dltdeck.core.model.DryRunTier;
import io.github.duhanguler.dltdeck.core.model.FailureCategory;
import io.github.duhanguler.dltdeck.core.model.FingerprintCluster;
import io.github.duhanguler.dltdeck.core.model.IngestedDltMessage;
import io.github.duhanguler.dltdeck.core.model.KeysetPage;
import io.github.duhanguler.dltdeck.core.model.MessageCoordinates;
import io.github.duhanguler.dltdeck.core.model.MessageStatus;
import io.github.duhanguler.dltdeck.core.model.PayloadFormat;
import io.github.duhanguler.dltdeck.core.model.PlanStripeLease;
import io.github.duhanguler.dltdeck.core.model.ReplayMode;
import io.github.duhanguler.dltdeck.core.model.ReplayOrderingMode;
import io.github.duhanguler.dltdeck.core.model.ReplayPlan;
import io.github.duhanguler.dltdeck.core.model.ReplayPlanStatus;
import io.github.duhanguler.dltdeck.core.patch.JsonPayloadPatcher;
import io.github.duhanguler.dltdeck.core.policy.AuditHashChain;
import io.github.duhanguler.dltdeck.core.policy.BlastRadiusGuard;
import io.github.duhanguler.dltdeck.core.policy.KeysetCursorCodec;
import io.github.duhanguler.dltdeck.core.policy.MakerCheckerPolicy;
import io.github.duhanguler.dltdeck.core.policy.ReplayHeaderSanitizer;
import io.github.duhanguler.dltdeck.core.policy.TargetTopicGuard;
import io.github.duhanguler.dltdeck.core.policy.TokenBucketRateLimiter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class CoreDomainUnitTest {

    static final class MutableClock extends Clock {
        private Instant current;

        MutableClock(Instant initial) {
            this.current = initial;
        }

        void advance(Duration delta) {
            this.current = this.current.plus(delta);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return current;
        }
    }

    @Test
    void actorContextScopedValueAndRolesWork() throws Exception {
        assertThat(ActorContext.currentOrSystem()).isEqualTo(ActorContext.SYSTEM_ACTOR);
        assertThatThrownBy(ActorContext::current).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> new ActorContext("   ", ActorRole.OPERATOR, null))
                .isInstanceOf(IllegalArgumentException.class);

        ActorContext op = new ActorContext("  alice ", ActorRole.OPERATOR, " corr-1 ");
        assertThat(op.actorId()).isEqualTo("alice");
        assertThat(op.correlationId()).isEqualTo("corr-1");

        AtomicReference<ActorContext> seen = new AtomicReference<>();
        ActorContext.runAs(op, () -> seen.set(ActorContext.current()));
        assertThat(seen.get()).isEqualTo(op);

        String result = ActorContext.callAs(
                new ActorContext("bob", ActorRole.APPROVER, " "),
                () -> ActorContext.current().actorId());
        assertThat(result).isEqualTo("bob");

        assertThat(ActorRole.VIEWER.canView()).isTrue();
        assertThat(ActorRole.VIEWER.canOperate()).isFalse();
        assertThat(ActorRole.OPERATOR.canOperate()).isTrue();
        assertThat(ActorRole.OPERATOR.canApprove()).isFalse();
        assertThat(ActorRole.APPROVER.canApprove()).isTrue();
        assertThat(ActorRole.APPROVER.canUnmaskPii()).isTrue();
        assertThat(ActorRole.MCP_AGENT.canApprove()).isFalse();
        assertThat(ActorRole.MCP_AGENT.privilegeLevel()).isEqualTo(1);
    }

    @Test
    void payloadCodecDetectsConfluentBinaryTombstoneJsonTextAndCloudEvents() {
        DefaultPayloadCodec codec = new DefaultPayloadCodec();
        Map<String, String> ceHeaders = Map.of(
                "CE_TYPE", "order.payment.failed",
                "ce_id", "evt-42",
                "ce_source", "/payments");

        // Confluent Wire Format: 0x00 + 4-byte schemaId (42) + payload
        byte[] confluent = ByteBuffer.allocate(9).put((byte) 0x00).putInt(42).putInt(0x11223344).array();
        PayloadCodec.DecodedPayload confluentDecoded = codec.decode(confluent, ceHeaders, false);
        assertThat(confluentDecoded.format()).isEqualTo(PayloadFormat.CONFLUENT_SCHEMA_REGISTRY);
        assertThat(confluentDecoded.patchable()).isFalse();
        assertThat(confluentDecoded.schemaId()).isEqualTo(42);
        assertThat(confluentDecoded.cloudEventType()).isEqualTo("order.payment.failed");
        assertThat(confluentDecoded.cloudEventId()).isEqualTo("evt-42");
        assertThat(confluentDecoded.cloudEventSource()).isEqualTo("/payments");
        assertThat(confluentDecoded.displayPreview()).contains("NOT_PATCHABLE");

        // Tombstone
        PayloadCodec.DecodedPayload tombstone = codec.decode(null, Map.of(), false);
        assertThat(tombstone.format()).isEqualTo(PayloadFormat.TOMBSTONE);
        assertThat(tombstone.patchable()).isFalse();

        // Null with deser exception
        PayloadCodec.DecodedPayload nullDeser = codec.decode(null, Map.of(), true);
        assertThat(nullDeser.format()).isEqualTo(PayloadFormat.BINARY_HEX);

        // Valid JSON
        byte[] json = "{\"orderId\":\"ORD-1\",\"amount\":99.5}".getBytes(StandardCharsets.UTF_8);
        PayloadCodec.DecodedPayload jsonDecoded = codec.decode(json, Map.of(), false);
        assertThat(jsonDecoded.format()).isEqualTo(PayloadFormat.JSON);
        assertThat(jsonDecoded.patchable()).isTrue();

        // Malformed JSON -> UTF8_TEXT, not patchable
        byte[] brokenJson = "{broken".getBytes(StandardCharsets.UTF_8);
        PayloadCodec.DecodedPayload brokenDecoded = codec.decode(brokenJson, Map.of(), true);
        assertThat(brokenDecoded.format()).isEqualTo(PayloadFormat.UTF8_TEXT);
        assertThat(brokenDecoded.patchable()).isFalse();

        // Empty payload
        assertThat(codec.decode(new byte[0], Map.of(), false).format()).isEqualTo(PayloadFormat.UTF8_TEXT);

        // Invalid UTF-8 bytes -> BINARY_HEX
        byte[] binary = new byte[] {(byte) 0xFF, (byte) 0xFE, (byte) 0x01, (byte) 0x80};
        PayloadCodec.DecodedPayload binDecoded = codec.decode(binary, Map.of(), false);
        assertThat(binDecoded.format()).isEqualTo(PayloadFormat.BINARY_HEX);
        assertThat(binDecoded.patchable()).isFalse();

        // Control chars inside UTF-8 -> BINARY_HEX
        byte[] ctrl = new byte[] {'a', 0x07, 'b'};
        assertThat(codec.decode(ctrl, Map.of(), false).format()).isEqualTo(PayloadFormat.BINARY_HEX);

        // Oversized binary & text truncation
        byte[] bigBin = new byte[700];
        bigBin[0] = (byte) 0xFF;
        assertThat(codec.decode(bigBin, Map.of(), false).displayPreview()).contains("bytes)");
        byte[] bigText = "x".repeat(20_000).getBytes(StandardCharsets.UTF_8);
        assertThat(codec.decode(bigText, Map.of(), false).displayPreview()).contains("truncated");
    }

    @Test
    void exceptionFingerprinterCollapsesVolatileTokensAndClassifiesCategories() {
        ExceptionFingerprinter fp = new ExceptionFingerprinter();
        String stack1 = """
                org.springframework.kafka.listener.ListenerExecutionFailedException: Listener failed
                    at org.springframework.kafka.listener.KafkaMessageListenerContainer.invoke(Container.java:10)
                    at com.acme.payment.PaymentHandler$$SpringCGLIB$$1234.handle(PaymentHandler.java:42)
                    at com.acme.payment.OrderProcessor.process(OrderProcessor.java:88)
                Caused by: jakarta.validation.ConstraintViolationException: Order 550e8400-e29b-41d4-a716-446655440000 at 2026-11-24T19:00:00Z failed amount=12345 @deadbeef
                    at com.acme.payment.Validator.check(Validator.java:19)
                """;
        String stack2 = """
                org.springframework.kafka.listener.ListenerExecutionFailedException: Listener failed
                    at org.springframework.kafka.listener.KafkaMessageListenerContainer.invoke(Container.java:10)
                    at com.acme.payment.PaymentHandler$$SpringCGLIB$$9999.handle(PaymentHandler.java:47)
                    at com.acme.payment.OrderProcessor.process(OrderProcessor.java:91)
                Caused by: jakarta.validation.ConstraintViolationException: Order 11111111-2222-3333-4444-555555555555 at 2026-11-24T20:15:30Z failed amount=98765 @cafebabe
                    at com.acme.payment.Validator.check(Validator.java:25)
                """;

        var r1 = fp.fingerprint(
                "org.springframework.kafka.listener.ListenerExecutionFailedException",
                null,
                "Order 550e8400-e29b-41d4-a716-446655440000 at 2026-11-24T19:00:00Z failed id=987654",
                stack1);
        var r2 = fp.fingerprint(
                "org.springframework.kafka.listener.ListenerExecutionFailedException",
                null,
                "Order 11111111-2222-3333-4444-555555555555 at 2026-11-24T20:15:30Z failed id=123456",
                stack2);

        assertThat(r1.fingerprint()).hasSize(64).isEqualTo(r2.fingerprint());
        assertThat(r1.rootCauseFqcn()).isEqualTo("jakarta.validation.ConstraintViolationException");
        assertThat(r1.failureCategory()).isEqualTo(FailureCategory.VALIDATION);

        assertThat(fp.fingerprint("org.springframework.kafka.support.serializer.DeserializationException", null, "bad json", null)
                        .failureCategory())
                .isEqualTo(FailureCategory.DESERIALIZATION);
        assertThat(fp.fingerprint("io.confluent.kafka.serializers.SchemaRegistryException", null, "unknown magic byte", null)
                        .failureCategory())
                .isEqualTo(FailureCategory.SCHEMA_MISMATCH);
        assertThat(fp.fingerprint("java.net.SocketTimeoutException", null, "503 ServiceUnavailable", null)
                        .failureCategory())
                .isEqualTo(FailureCategory.TRANSIENT_DOWNSTREAM);
        assertThat(fp.fingerprint("org.springframework.security.access.AccessDeniedException", null, "403", null)
                        .failureCategory())
                .isEqualTo(FailureCategory.AUTH_PERMISSION);
        assertThat(fp.fingerprint("java.lang.NullPointerException", null, "null amount", null)
                        .failureCategory())
                .isEqualTo(FailureCategory.POISON_DATA);
        assertThat(fp.fingerprint("com.acme.CustomBoom", null, "mystery", "at java.base/java.lang.Thread.run(Thread.java:1)")
                        .failureCategory())
                .isEqualTo(FailureCategory.UNKNOWN);
        assertThatThrownBy(() -> new ExceptionFingerprinter(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void piiRedactorValidatesChecksumsAndCsvSanitizerNeutralizesFormulas() {
        PiiRedactor redactor = new PiiRedactor();

        // Valid TCKN: 10000000146 -> odd=1+0+0+0+1=2, even=0+0+0+0=0 -> d10=(14-0)%10=4; sum10=6 -> d11=6
        assertThat(PiiRedactor.isValidTckn("10000000146")).isTrue();
        assertThat(PiiRedactor.isValidTckn("10000000147")).isFalse();
        assertThat(PiiRedactor.isValidTckn("01234567890")).isFalse();

        // Valid Turkish IBAN: TR330006100519786457841326
        assertThat(PiiRedactor.isValidIban("TR33 0006 1005 1978 6457 8413 26")).isTrue();
        assertThat(PiiRedactor.isValidIban("TR99 0006 1005 1978 6457 8413 26")).isFalse();

        // Valid Luhn PAN: 4532015112830366
        assertThat(PiiRedactor.isValidLuhnPan("4532-0151-1283-0366")).isTrue();
        assertThat(PiiRedactor.isValidLuhnPan("4532-0151-1283-0367")).isFalse();

        String raw = """
                {"tckn":"10000000146","note":"ID 10000000146 IBAN TR330006100519786457841326 card 4532-0151-1283-0366 orderId 12345678901 email duhan@example.com phone +90 532 123 45 67 jwt Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyMSJ9.sig123456789","password":"topsecret"}
                """;
        String redacted = redactor.redactText(raw);
        assertThat(redacted)
                .doesNotContain("10000000146")
                .doesNotContain("TR330006100519786457841326")
                .doesNotContain("4532-0151-1283-0366")
                .doesNotContain("topsecret")
                .doesNotContain("duhan@example.com")
                .doesNotContain("+90 532 123 45 67")
                .doesNotContain("eyJhbGciOiJIUzI1NiJ9")
                .contains("[REDACTED_SECRET]")
                .contains("[REDACTED_IBAN]")
                .contains("[REDACTED_PAN]")
                .contains("[REDACTED_TCKN]")
                .contains("d***@example.com")
                .contains("[REDACTED_PHONE]")
                .contains("[REDACTED_JWT]")
                .contains("12345678901"); // invalid TCKN order ID preserved!

        Map<String, String> headers = redactor.redactHeaders(
                Map.of("Authorization", "Bearer abc", "X-User-Email", "a@b.io", "X-Order-Id", "ORD-9"));
        assertThat(headers.get("Authorization")).isEqualTo("[REDACTED_HEADER]");
        assertThat(headers.get("X-User-Email")).isEqualTo("*@b.io");
        assertThat(headers.get("X-Order-Id")).isEqualTo("ORD-9");

        // CSV formula injection
        assertThat(CsvExportSanitizer.sanitizeCell("=CMD|' /C calc'!A0")).isEqualTo("'=CMD|' /C calc'!A0");
        assertThat(CsvExportSanitizer.sanitizeCell("+SUM(A1:A2)")).isEqualTo("'+SUM(A1:A2)");
        assertThat(CsvExportSanitizer.sanitizeCell("-1+1")).isEqualTo("'-1+1");
        assertThat(CsvExportSanitizer.sanitizeCell("@SUM(1,2)")).isEqualTo("\"'@SUM(1,2)\"");
        assertThat(CsvExportSanitizer.sanitizeCell("hello, \"world\"")).isEqualTo("\"hello, \"\"world\"\"\"");
        assertThat(CsvExportSanitizer.sanitizeCell(null)).isEmpty();
    }

    @Test
    void jsonPayloadPatcherEnforcesRfc7396FormatAndResourceBounds() {
        JsonPayloadPatcher patcher = new JsonPayloadPatcher();
        byte[] original = "{\"orderId\":\"ORD-1\",\"currency\":\"TRL\",\"nested\":{\"a\":1,\"b\":2},\"obsolete\":true}"
                .getBytes(StandardCharsets.UTF_8);
        String patch = "{\"currency\":\"TRY\",\"nested\":{\"b\":null,\"c\":3},\"obsolete\":null,\"added\":\"ok\"}";

        var result = patcher.applyMergePatch(PayloadFormat.JSON, original, patch);
        String patchedStr = new String(result.patchedBytes(), StandardCharsets.UTF_8);
        assertThat(patchedStr)
                .contains("\"currency\":\"TRY\"")
                .contains("\"c\":3")
                .contains("\"added\":\"ok\"")
                .doesNotContain("\"obsolete\"")
                .doesNotContain("\"b\":2");
        assertThat(result.diffs()).hasSize(5);

        // Non-JSON payload refused as NOT_PATCHABLE
        assertThatThrownBy(() -> patcher.applyMergePatch(
                        PayloadFormat.CONFLUENT_SCHEMA_REGISTRY, new byte[] {0, 0, 0, 0, 1}, "{}"))
                .isInstanceOf(ValidationDomainException.class)
                .extracting("errorCode")
                .isEqualTo("NOT_PATCHABLE");

        // Depth bomb rejected
        String deep = "{\"a\":".repeat(40) + "1" + "}".repeat(40);
        assertThatThrownBy(() -> patcher.applyMergePatch(PayloadFormat.JSON, deep.getBytes(StandardCharsets.UTF_8), "{}"))
                .isInstanceOf(ValidationDomainException.class)
                .extracting("errorCode")
                .isEqualTo("JSON_DEPTH_EXCEEDED");

        // Size bomb rejected
        JsonPayloadPatcher tiny = new JsonPayloadPatcher(tools.jackson.databind.json.JsonMapper.builder().build(), 32, 8);
        assertThatThrownBy(() -> tiny.applyMergePatch(PayloadFormat.JSON, original, "{}"))
                .isInstanceOf(ValidationDomainException.class)
                .extracting("errorCode")
                .isEqualTo("PAYLOAD_TOO_LARGE");
    }

    @Test
    void replayHeadersBlastRadiusRateLimiterMakerCheckerAuditCursorAndTopicsWork() {
        MutableClock clock = new MutableClock(Instant.parse("2026-11-24T12:00:00.123456789Z"));

        // ReplayHeaderSanitizer
        ReplayHeaderSanitizer sanitizer = new ReplayHeaderSanitizer(clock);
        UUID planId = UUID.randomUUID();
        MessageCoordinates coords = new MessageCoordinates("orders.dlt", 2, 105L);
        Map<String, String> stamped = sanitizer.sanitizeAndStamp(
                Map.of(
                        "kafka_dlt-exception-message", "boom",
                        "retry_topic-attempts", "3",
                        "springDeserializerExceptionValue", "bytes",
                        "traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                        "x-tenant-id", "tr-retail"),
                planId,
                coords,
                7L,
                1);
        assertThat(stamped)
                .doesNotContainKeys(
                        "kafka_dlt-exception-message", "retry_topic-attempts", "springDeserializerExceptionValue")
                .containsEntry("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .containsEntry("x-tenant-id", "tr-retail")
                .containsEntry(ReplayHeaderSanitizer.HEADER_FENCING_TOKEN, "7")
                .containsEntry(ReplayHeaderSanitizer.HEADER_REPLAY_HOPS, "2")
                .containsEntry(ReplayHeaderSanitizer.HEADER_ORIGIN_COORDS, "orders.dlt[2]@105")
                .containsEntry(
                        ReplayHeaderSanitizer.HEADER_IDEMPOTENCY_KEY,
                        ReplayHeaderSanitizer.computeIdempotencyKey(planId, coords));

        // BlastRadiusGuard (Java 25 Gatherers.windowSliding)
        BlastRadiusGuard guard = new BlastRadiusGuard(clock, 4, 0.5, Duration.ofMinutes(5));
        assertThat(guard.evaluate(planId).tripped()).isFalse();
        assertThat(guard.recordPublished(planId).tripped()).isFalse();
        assertThat(guard.recordPublished(planId).tripped()).isFalse();
        assertThat(guard.recordCorrelatedReDlt(planId).tripped()).isFalse();
        assertThat(guard.recordCorrelatedReDlt(planId).tripped()).isTrue();
        clock.advance(Duration.ofMinutes(6));
        assertThat(guard.evaluate(planId).tripped()).isFalse();
        guard.reset(planId);

        // TokenBucketRateLimiter (Clock-driven, zero Thread.sleep)
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(clock, 10.0, 2.0);
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isFalse();
        assertThat(limiter.delayUntilAvailable(1)).isGreaterThan(Duration.ZERO);
        clock.advance(Duration.ofMillis(200));
        assertThat(limiter.availableTokens()).isGreaterThanOrEqualTo(1.9);
        assertThat(limiter.tryAcquire(2)).isTrue();

        // MakerCheckerPolicy + TOCTOU planSpecHash
        MakerCheckerPolicy mc = new MakerCheckerPolicy();
        String specHash = mc.computePlanSpecHash(
                "Replay Orders", "fp1", "orders.dlt", "orders", ReplayOrderingMode.PRESERVE_KEY_ORDER, null, 50, 100, 10);
        ReplayPlan pendingPlan = new ReplayPlan(
                planId,
                "Replay Orders",
                "fp1",
                "orders.dlt",
                "orders",
                ReplayOrderingMode.PRESERVE_KEY_ORDER,
                null,
                50,
                100,
                10,
                specHash,
                ReplayPlanStatus.PENDING_APPROVAL,
                true,
                "operator-1",
                null,
                null,
                0,
                0,
                0,
                0,
                0,
                0,
                clock.instant(),
                null,
                null);
        // Self-approval blocked
        assertThatThrownBy(() -> mc.verifyApprovalAllowed(
                        pendingPlan, new ActorContext("operator-1", ActorRole.APPROVER, null), specHash))
                .isInstanceOf(ForbiddenActionException.class)
                .extracting("errorCode")
                .isEqualTo("SELF_APPROVAL_FORBIDDEN");
        // Non-approver blocked
        assertThatThrownBy(() -> mc.verifyApprovalAllowed(
                        pendingPlan, new ActorContext("other-op", ActorRole.OPERATOR, null), specHash))
                .isInstanceOf(ForbiddenActionException.class);
        // TOCTOU mismatch blocked
        assertThatThrownBy(() -> mc.verifyApprovalAllowed(
                        pendingPlan, new ActorContext("approver-1", ActorRole.APPROVER, null), "badhash"))
                .isInstanceOf(PlanConflictException.class)
                .extracting("errorCode")
                .isEqualTo("EXPECTED_SPEC_HASH_MISMATCH");
        // Independent approver succeeds
        mc.verifyApprovalAllowed(pendingPlan, new ActorContext("approver-1", ActorRole.APPROVER, null), specHash);

        // AuditHashChain + Merkle root
        AuditHashChain chain = new AuditHashChain();
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        UUID id3 = UUID.randomUUID();
        String merkle = chain.computeMerkleRoot(List.of(id1, id2, id3));
        assertThat(merkle).hasSize(64);
        assertThat(chain.computeMerkleRoot(List.of())).hasSize(64);

        Instant t1 = AuditHashChain.normalizeTimestamp(clock.instant());
        String h1 = chain.computeEntryHash(
                AuditHashChain.GENESIS_HASH, id1, "op", "OPERATOR", "CREATE_PLAN", "PLAN", planId.toString(), null, "{}", t1);
        AuditEntry e1 = new AuditEntry(1L, id1, "op", "OPERATOR", "CREATE_PLAN", "PLAN", planId.toString(), null, "{}", AuditHashChain.GENESIS_HASH, h1, t1);
        String h2 = chain.computeEntryHash(
                h1, id2, "system", "SYSTEM", "BATCH_CHECKPOINT", "PLAN", planId.toString(), merkle, "{\"count\":3}", t1);
        AuditEntry e2 = new AuditEntry(2L, id2, "system", "SYSTEM", "BATCH_CHECKPOINT", "PLAN", planId.toString(), merkle, "{\"count\":3}", h1, h2, t1);

        AuditChainVerification ok = chain.verifyChain(List.of(e1, e2));
        assertThat(ok.valid()).isTrue();
        assertThat(ok.totalChecked()).isEqualTo(2);
        assertThat(ok.latestHash()).isEqualTo(h2);

        AuditEntry tampered = new AuditEntry(2L, id2, "evil", "SYSTEM", "BATCH_CHECKPOINT", "PLAN", planId.toString(), merkle, "{\"count\":3}", h1, h2, t1);
        AuditChainVerification bad = chain.verifyChain(List.of(e1, tampered));
        assertThat(bad.valid()).isFalse();
        assertThat(bad.firstInvalidSequenceNo()).isEqualTo(2L);

        // KeysetCursorCodec
        KeysetCursorCodec cursorCodec = new KeysetCursorCodec("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8));
        String encoded = cursorCodec.encode(clock.instant(), id1);
        KeysetCursorCodec.CursorToken decoded = cursorCodec.decode(encoded);
        assertThat(decoded.id()).isEqualTo(id1);
        assertThat(decoded.ingestedAt()).isEqualTo(t1);
        assertThatThrownBy(() -> cursorCodec.decode(encoded + "A"))
                .isInstanceOf(ValidationDomainException.class)
                .extracting("errorCode")
                .isEqualTo("INVALID_CURSOR");

        // TargetTopicGuard
        TargetTopicGuard.validateTargetTopicName("orders.events");
        assertThatThrownBy(() -> TargetTopicGuard.validateTargetTopicName("orders.dlt"))
                .isInstanceOf(ValidationDomainException.class)
                .extracting("errorCode")
                .isEqualTo("FORBIDDEN_DLT_TARGET");
        assertThatThrownBy(() -> TargetTopicGuard.validateTargetTopicName("__consumer_offsets"))
                .isInstanceOf(ValidationDomainException.class)
                .extracting("errorCode")
                .isEqualTo("FORBIDDEN_INTERNAL_TOPIC");
        assertThat(TargetTopicGuard.isExcludedFromDiscovery("__transaction_state")).isTrue();
        assertThat(TargetTopicGuard.isExcludedFromDiscovery("orders.canary")).isTrue();
        assertThat(TargetTopicGuard.isExcludedFromDiscovery("orders.dlt")).isFalse();

        // Model & JFR sanity checks
        assertThat(new ReplayMode.OriginalTopic()).isNotNull();
        assertThat(new ReplayMode.TargetTopic("orders").targetTopic()).isEqualTo("orders");
        assertThat(new ReplayMode.CanaryTopic("orders.canary").canaryTopic()).isEqualTo("orders.canary");
        assertThat(new ReplayMode.PatchedPayload("orders", "{\"a\":1}").mergePatchJson()).isEqualTo("{\"a\":1}");
        assertThat(MessageStatus.NEW.isReplayEligible()).isTrue();
        assertThat(MessageStatus.INDETERMINATE.requiresManualIntervention()).isTrue();
        assertThat(MessageStatus.PUBLISHED.isTerminal()).isTrue();
        assertThat(ReplayPlanStatus.APPROVED.isExecutable()).isTrue();
        assertThat(ReplayPlanStatus.COMPLETED.isTerminal()).isTrue();
        assertThat(new NotFoundException("NF", "missing").errorCode()).isEqualTo("NF");
        assertThat(new RateLimitExceededException("RL", "slow down").errorCode()).isEqualTo("RL");
        assertThat(new BatchMerkleCheckpoint(planId, 1, merkle, List.of(id1), t1).batchSize()).isEqualTo(1);
        assertThat(new KeysetPage<>(List.of("a"), null, false).items()).containsExactly("a");
        assertThat(new DryRunOutcome(DryRunTier.TIER_1_VALIDATE, true, "ok", List.of(), null, null, null, 2L).passed()).isTrue();
        assertThat(new PlanStripeLease(planId, 0, "pod-1", t1, 1L, PlanStripeLease.StripeStatus.ACTIVE).stripeId()).isZero();

        DltMessageSummary summary = new DltMessageSummary(
                id1, "orders.dlt", 0, 10L, "orders", 3, 5L, "cg", 99, "kfp", PayloadFormat.JSON, null, 12,
                "fp", FailureCategory.VALIDATION, 0, MessageStatus.NEW, null, 0L, null, null, null, null, t1, t1);
        assertThat(summary.dltCoordinates().format()).isEqualTo("orders.dlt[0]@10");
        assertThat(summary.stripeId(4)).isEqualTo(3);

        FingerprintCluster cluster = new FingerprintCluster("fp", "Ex", "Ex", "msg", "st", FailureCategory.VALIDATION, "rec", t1, t1, 1, 1);
        byte[] keyBytes = "k1".getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = "{}".getBytes(StandardCharsets.UTF_8);
        DltMessageDetail detail = new DltMessageDetail(summary, cluster, keyBytes, valBytes, "RAW", null, Map.of(), null, null, null);
        keyBytes[0] = 'X';
        assertThat(detail.messageKey()).containsExactly('k', '1');
        assertThat(detail.payload()).containsExactly('{', '}');

        IngestedDltMessage ingested = new IngestedDltMessage(
                id1, "orders.dlt", 0, 10L, "orders", 0, 1L, "cg", keyBytes, 1, "kfp", valBytes,
                PayloadFormat.JSON, null, 2, "fp", "Ex", "Ex", "m", "s", FailureCategory.UNKNOWN, "r",
                0, MessageStatus.NEW, null, Map.of(), t1);
        assertThat(ingested.payload()).containsExactly('{', '}');

        new DltIngestionBatchEvent().commit();
        new DltReplayRecordEvent().commit();
        new DltBlastRadiusTripEvent().commit();
        new DltIndeterminateRecoveryEvent().commit();
    }
}
