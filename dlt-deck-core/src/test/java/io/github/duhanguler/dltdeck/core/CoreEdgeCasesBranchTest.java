package io.github.duhanguler.dltdeck.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.duhanguler.dltdeck.core.codec.DefaultPayloadCodec;
import io.github.duhanguler.dltdeck.core.context.ActorContext;
import io.github.duhanguler.dltdeck.core.context.ActorRole;
import io.github.duhanguler.dltdeck.core.exception.ForbiddenActionException;
import io.github.duhanguler.dltdeck.core.exception.PlanConflictException;
import io.github.duhanguler.dltdeck.core.exception.ValidationDomainException;
import io.github.duhanguler.dltdeck.core.fingerprint.ExceptionFingerprinter;
import io.github.duhanguler.dltdeck.core.masking.CsvExportSanitizer;
import io.github.duhanguler.dltdeck.core.masking.PiiRedactor;
import io.github.duhanguler.dltdeck.core.model.DltMessageDetail;
import io.github.duhanguler.dltdeck.core.model.DltMessageSummary;
import io.github.duhanguler.dltdeck.core.model.FailureCategory;
import io.github.duhanguler.dltdeck.core.model.FingerprintCluster;
import io.github.duhanguler.dltdeck.core.model.IngestedDltMessage;
import io.github.duhanguler.dltdeck.core.model.MessageCoordinates;
import io.github.duhanguler.dltdeck.core.model.MessageStatus;
import io.github.duhanguler.dltdeck.core.model.PayloadFormat;
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
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class CoreEdgeCasesBranchTest {

    @Test
    void fingerprinterCoversAllClassificationBranchesAndNullInputs() {
        ExceptionFingerprinter fp = new ExceptionFingerprinter(2);

        // Null / blank inputs
        var blank = fp.fingerprint(null, null, null, null);
        assertThat(blank.exceptionFqcn()).isNull();
        assertThat(blank.rootCauseFqcn()).isNull();
        assertThat(blank.canonicalStacktrace()).isNull();
        assertThat(blank.failureCategory()).isEqualTo(FailureCategory.UNKNOWN);

        // Explicit rootCauseFqcn + very long message truncation + framework frames
        String noisyStack = """
                com.acme.Top: boom
                    at java.base/java.lang.Object.wait(Native Method)
                    at jdk.internal.reflect.NativeMethodAccessorImpl.invoke(Native.java:1)
                    at sun.reflect.DelegatingMethodAccessorImpl.invoke(Del.java:2)
                    at org.springframework.cglib.proxy.MethodProxy.invoke(MP.java:3)
                    at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(R.java:4)
                    at org.springframework.kafka.listener.RecordMessagingMessageListenerAdapter.invoke(A.java:5)
                    at com.acme.Service.a(Service.java:10)
                    at com.acme.Service.b(Service.java:20)
                    at com.acme.Service.c(Service.java:30)
                Caused by: com.acme.InnerNoColon
                """;
        var withRoot = fp.fingerprint("com.acme.Top", "com.acme.ExplicitRoot", "x".repeat(1200), noisyStack);
        assertThat(withRoot.rootCauseFqcn()).isEqualTo("com.acme.ExplicitRoot");
        assertThat(withRoot.sampleMessage()).hasSize(1024);

        // Caused by without colon
        var noColon = fp.fingerprint("com.acme.Top", " ", "msg", "Caused by: com.acme.InnerNoColon\n");
        assertThat(noColon.rootCauseFqcn()).isEqualTo("com.acme.InnerNoColon");

        // All classification branches
        assertThat(fp.fingerprint("x", null, "JsonParseException", null).failureCategory())
                .isEqualTo(FailureCategory.DESERIALIZATION);
        assertThat(fp.fingerprint("x", null, "StreamReadException", null).failureCategory())
                .isEqualTo(FailureCategory.DESERIALIZATION);
        assertThat(fp.fingerprint("x", null, "MismatchedInputException", null).failureCategory())
                .isEqualTo(FailureCategory.DESERIALIZATION);
        assertThat(fp.fingerprint("x", null, "SerializationException", null).failureCategory())
                .isEqualTo(FailureCategory.DESERIALIZATION);
        assertThat(fp.fingerprint("x", null, "InvalidFormatException", null).failureCategory())
                .isEqualTo(FailureCategory.DESERIALIZATION);
        assertThat(fp.fingerprint("x", null, "EOFException", null).failureCategory())
                .isEqualTo(FailureCategory.DESERIALIZATION);

        assertThat(fp.fingerprint("x", null, "incompatible schema", null).failureCategory())
                .isEqualTo(FailureCategory.SCHEMA_MISMATCH);
        assertThat(fp.fingerprint("x", null, "AvroTypeException", null).failureCategory())
                .isEqualTo(FailureCategory.SCHEMA_MISMATCH);
        assertThat(fp.fingerprint("x", null, "InvalidProtocolBufferException", null).failureCategory())
                .isEqualTo(FailureCategory.SCHEMA_MISMATCH);

        assertThat(fp.fingerprint("x", null, "ValidationException", null).failureCategory())
                .isEqualTo(FailureCategory.VALIDATION);
        assertThat(fp.fingerprint("x", null, "MethodArgumentNotValidException", null).failureCategory())
                .isEqualTo(FailureCategory.VALIDATION);
        assertThat(fp.fingerprint("x", null, "IllegalArgumentException", null).failureCategory())
                .isEqualTo(FailureCategory.VALIDATION);

        assertThat(fp.fingerprint("x", null, "ConnectException", null).failureCategory())
                .isEqualTo(FailureCategory.TRANSIENT_DOWNSTREAM);
        assertThat(fp.fingerprint("x", null, "SocketException", null).failureCategory())
                .isEqualTo(FailureCategory.TRANSIENT_DOWNSTREAM);
        assertThat(fp.fingerprint("x", null, "504 gateway", null).failureCategory())
                .isEqualTo(FailureCategory.TRANSIENT_DOWNSTREAM);
        assertThat(fp.fingerprint("x", null, "429 too many", null).failureCategory())
                .isEqualTo(FailureCategory.TRANSIENT_DOWNSTREAM);
        assertThat(fp.fingerprint("x", null, "deadlock detected", null).failureCategory())
                .isEqualTo(FailureCategory.TRANSIENT_DOWNSTREAM);
        assertThat(fp.fingerprint("x", null, "TransientDataAccessException", null).failureCategory())
                .isEqualTo(FailureCategory.TRANSIENT_DOWNSTREAM);

        assertThat(fp.fingerprint("x", null, "AuthorizationException", null).failureCategory())
                .isEqualTo(FailureCategory.AUTH_PERMISSION);
        assertThat(fp.fingerprint("x", null, "AuthenticationFailed", null).failureCategory())
                .isEqualTo(FailureCategory.AUTH_PERMISSION);
        assertThat(fp.fingerprint("x", null, "Unauthorized", null).failureCategory())
                .isEqualTo(FailureCategory.AUTH_PERMISSION);
        assertThat(fp.fingerprint("x", null, "Forbidden", null).failureCategory())
                .isEqualTo(FailureCategory.AUTH_PERMISSION);
        assertThat(fp.fingerprint("x", null, "HTTP 401", null).failureCategory())
                .isEqualTo(FailureCategory.AUTH_PERMISSION);

        assertThat(fp.fingerprint("x", null, "IllegalStateException", null).failureCategory())
                .isEqualTo(FailureCategory.POISON_DATA);
        assertThat(fp.fingerprint("x", null, "NumberFormatException", null).failureCategory())
                .isEqualTo(FailureCategory.POISON_DATA);
        assertThat(fp.fingerprint("x", null, "ArithmeticException", null).failureCategory())
                .isEqualTo(FailureCategory.POISON_DATA);
        assertThat(fp.fingerprint("x", null, "DataIntegrityViolationException", null).failureCategory())
                .isEqualTo(FailureCategory.POISON_DATA);
    }

    @Test
    void piiRedactorPatcherCodecAndCsvCoverAllErrorBranches() {
        PiiRedactor r = new PiiRedactor();
        assertThat(r.redactText("")).isEmpty();
        assertThat(PiiRedactor.isValidTckn("123")).isFalse();
        assertThat(PiiRedactor.isValidTckn("10000000a46")).isFalse();
        assertThat(PiiRedactor.isValidTckn("10000000156")).isFalse();
        assertThat(PiiRedactor.isValidIban("TR12")).isFalse();
        assertThat(PiiRedactor.isValidIban("11330006100519786457841326")).isFalse();
        assertThat(PiiRedactor.isValidIban("TRAA0006100519786457841326")).isFalse();
        assertThat(PiiRedactor.isValidIban("TR33000610051978645784132!")).isFalse();
        assertThat(PiiRedactor.isValidLuhnPan("1234")).isFalse();
        assertThat(PiiRedactor.isValidLuhnPan("453201511283036a")).isFalse();
        assertThat(r.redactHeaders(Map.of(
                        "X-Custom-Secret", "s",
                        "X-My-Token", "t",
                        "X-Db-Password", "p",
                        "X-My-Authorization", "a")))
                .allSatisfy((_, v) -> assertThat(v).isEqualTo("[REDACTED_HEADER]"));

        // JsonPayloadPatcher edge branches
        assertThatThrownBy(() -> new JsonPayloadPatcher(JsonMapper.builder().build(), 0, 5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JsonPayloadPatcher(JsonMapper.builder().build(), 100, 0))
                .isInstanceOf(IllegalArgumentException.class);
        JsonPayloadPatcher patcher = new JsonPayloadPatcher(JsonMapper.builder().build(), 128, 6);
        assertThatThrownBy(() -> patcher.applyMergePatch(PayloadFormat.JSON, null, "{}"))
                .isInstanceOf(ValidationDomainException.class);
        assertThatThrownBy(() -> patcher.applyMergePatch(PayloadFormat.JSON, new byte[0], "{}"))
                .isInstanceOf(ValidationDomainException.class);
        assertThatThrownBy(() -> patcher.applyMergePatch(PayloadFormat.JSON, "{}".getBytes(StandardCharsets.UTF_8), "a".repeat(200)))
                .isInstanceOf(ValidationDomainException.class);
        assertThatThrownBy(() -> patcher.applyMergePatch(PayloadFormat.JSON, "{bad".getBytes(StandardCharsets.UTF_8), "{}"))
                .isInstanceOf(ValidationDomainException.class);
        assertThatThrownBy(() -> patcher.applyMergePatch(PayloadFormat.JSON, "{\"a\":1}".getBytes(StandardCharsets.UTF_8), "{\"b\":\"" + "x".repeat(120) + "\"}"))
                .isInstanceOf(ValidationDomainException.class)
                .extracting("errorCode")
                .isEqualTo("PATCHED_PAYLOAD_TOO_LARGE");

        // Array + escaped quote string + non-object patch + unchanged JSON
        var arrPatch = patcher.applyMergePatch(
                PayloadFormat.JSON,
                "{\"text\":\"a\\\"b\\\\c\",\"arr\":[1,2]}".getBytes(StandardCharsets.UTF_8),
                "{\"arr\":[1,3]}");
        assertThat(arrPatch.diffs()).hasSize(1);
        var replaceRoot = patcher.applyMergePatch(
                PayloadFormat.JSON, "{\"a\":1}".getBytes(StandardCharsets.UTF_8), "[1,2]");
        assertThat(new String(replaceRoot.patchedBytes(), StandardCharsets.UTF_8)).isEqualTo("[1,2]");
        var replaceArrayWithObj = patcher.applyMergePatch(
                PayloadFormat.JSON, "[1,2]".getBytes(StandardCharsets.UTF_8), "{\"k\":\"v\"}");
        assertThat(new String(replaceArrayWithObj.patchedBytes(), StandardCharsets.UTF_8)).contains("\"k\":\"v\"");

        // DefaultPayloadCodec array JSON + zero schemaId
        DefaultPayloadCodec codec = new DefaultPayloadCodec();
        assertThat(codec.decode("[1,2,3]".getBytes(StandardCharsets.UTF_8), Map.of(), false).format())
                .isEqualTo(PayloadFormat.JSON);
        byte[] zeroSchema = ByteBuffer.allocate(6).put((byte) 0x00).putInt(0).put((byte) 1).array();
        assertThat(codec.decode(zeroSchema, Map.of(), false).format()).isEqualTo(PayloadFormat.BINARY_HEX);

        // CsvExportSanitizer tab/cr/newline
        assertThat(CsvExportSanitizer.sanitizeCell("\tabc")).isEqualTo("'\tabc");
        assertThat(CsvExportSanitizer.sanitizeCell("\rabc")).isEqualTo("\"'\rabc\"");
        assertThat(CsvExportSanitizer.sanitizeCell("line1\nline2")).isEqualTo("\"line1\nline2\"");
    }

    @Test
    void policiesStatusesAndModelsCoverAllRemainingBranches() {
        var clock = new CoreDomainUnitTest.MutableClock(Instant.parse("2026-11-24T12:00:00Z"));
        UUID planId = UUID.randomUUID();
        Instant now = clock.instant();

        // MakerCheckerPolicy execution integrity & status checks
        MakerCheckerPolicy mc = new MakerCheckerPolicy();
        String hash = mc.computePlanSpecHash("n", null, "t.dlt", "t", ReplayOrderingMode.UNORDERED_FAST, null, 10, 10, 1);
        ReplayPlan approvedNoApprover = new ReplayPlan(
                planId, "n", null, "t.dlt", "t", ReplayOrderingMode.UNORDERED_FAST, null, 10, 10, 1,
                hash, ReplayPlanStatus.APPROVED, true, "op", null, null, 0, 0, 0, 0, 0, 0, now, null, null);
        assertThatThrownBy(() -> mc.verifyExecutionIntegrity(approvedNoApprover))
                .isInstanceOf(ForbiddenActionException.class);

        ReplayPlan selfApproved = new ReplayPlan(
                planId, "n", null, "t.dlt", "t", ReplayOrderingMode.UNORDERED_FAST, null, 10, 10, 1,
                hash, ReplayPlanStatus.APPROVED, true, "op", "OP", null, 0, 0, 0, 0, 0, 0, now, now, null);
        assertThatThrownBy(() -> mc.verifyExecutionIntegrity(selfApproved))
                .isInstanceOf(ForbiddenActionException.class);

        ReplayPlan mutated = new ReplayPlan(
                planId, "n", null, "t.dlt", "other-topic", ReplayOrderingMode.UNORDERED_FAST, null, 10, 10, 1,
                hash, ReplayPlanStatus.APPROVED, false, "op", null, null, 0, 0, 0, 0, 0, 0, now, null, null);
        assertThatThrownBy(() -> mc.verifyExecutionIntegrity(mutated))
                .isInstanceOf(PlanConflictException.class);

        ReplayPlan validExec = new ReplayPlan(
                planId, "n", null, "t.dlt", "t", ReplayOrderingMode.UNORDERED_FAST, null, 10, 10, 1,
                hash, ReplayPlanStatus.RUNNING, true, "op", "checker", null, 0, 0, 0, 0, 0, 0, now, now, null);
        mc.verifyExecutionIntegrity(validExec);

        assertThatThrownBy(() -> mc.verifyApprovalAllowed(validExec, new ActorContext("checker", ActorRole.APPROVER, null), hash))
                .isInstanceOf(PlanConflictException.class);
        ReplayPlan mutatedPending = new ReplayPlan(
                planId, "n", null, "t.dlt", "t", ReplayOrderingMode.UNORDERED_FAST, null, 999, 10, 1,
                hash, ReplayPlanStatus.PENDING_APPROVAL, true, "op", null, null, 0, 0, 0, 0, 0, 0, now, null, null);
        assertThatThrownBy(() -> mc.verifyApprovalAllowed(mutatedPending, new ActorContext("checker", ActorRole.APPROVER, null), hash))
                .isInstanceOf(PlanConflictException.class);
        assertThatThrownBy(() -> mc.verifyExecutionIntegrity(mutatedPending))
                .isInstanceOf(PlanConflictException.class);

        // Enum helper branches
        for (MessageStatus s : MessageStatus.values()) {
            s.isReplayEligible();
            s.requiresManualIntervention();
            s.isTerminal();
        }
        for (ReplayPlanStatus s : ReplayPlanStatus.values()) {
            s.isExecutable();
            s.isTerminal();
        }
        for (ActorRole role : ActorRole.values()) {
            role.canView();
            role.canOperate();
            role.canApprove();
            role.canUnmaskPii();
            role.privilegeLevel();
        }

        // TokenBucketRateLimiter validation & zero-wait branches
        assertThatThrownBy(() -> new TokenBucketRateLimiter(clock, 0, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TokenBucketRateLimiter(clock, 1, 0.5)).isInstanceOf(IllegalArgumentException.class);
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(clock, 10, 5);
        assertThatThrownBy(() -> limiter.tryAcquire(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> limiter.delayUntilAvailable(0)).isInstanceOf(IllegalArgumentException.class);
        assertThat(limiter.delayUntilAvailable(1)).isEqualTo(Duration.ZERO);

        // BlastRadiusGuard constructor & maxHistory trimming
        assertThatThrownBy(() -> new BlastRadiusGuard(clock, 1, 0.5, Duration.ofMinutes(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BlastRadiusGuard(clock, 2, 0.0, Duration.ofMinutes(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BlastRadiusGuard(clock, 2, 0.5, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        BlastRadiusGuard guard = new BlastRadiusGuard(clock, 2, 0.5, Duration.ofMinutes(5));
        for (int i = 0; i < 12; i++) {
            guard.recordPublished(planId);
        }
        assertThat(guard.evaluate(planId).tripped()).isFalse();

        // KeysetCursorCodec edge errors
        assertThatThrownBy(() -> new KeysetCursorCodec("short".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class);
        KeysetCursorCodec cc = new KeysetCursorCodec("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> cc.decode("nodot")).isInstanceOf(ValidationDomainException.class);
        assertThatThrownBy(() -> cc.decode("a.b.c")).isInstanceOf(ValidationDomainException.class);
        assertThatThrownBy(() -> cc.decode("!!!.@@@")).isInstanceOf(ValidationDomainException.class);
        String fakePayload = Base64.getUrlEncoder().withoutPadding().encodeToString("bad".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> cc.decode(fakePayload + "." + fakePayload)).isInstanceOf(ValidationDomainException.class);

        // TargetTopicGuard edge branches
        assertThatThrownBy(() -> TargetTopicGuard.validateTargetTopicName("  ")).isInstanceOf(ValidationDomainException.class);
        assertThatThrownBy(() -> TargetTopicGuard.validateTargetTopicName("..")).isInstanceOf(ValidationDomainException.class);
        assertThatThrownBy(() -> TargetTopicGuard.validateTargetTopicName("bad topic!")).isInstanceOf(ValidationDomainException.class);
        assertThat(TargetTopicGuard.isExcludedFromDiscovery("orders-canary")).isTrue();

        // AuditHashChain broken prevHash link
        AuditHashChain chain = new AuditHashChain();
        var badPrev = chain.verifyChain(List.of(new io.github.duhanguler.dltdeck.core.model.AuditEntry(
                1L, UUID.randomUUID(), "a", "SYSTEM", "ACT", "T", "1", null, "{}", "wrong-prev", "h", now)));
        assertThat(badPrev.valid()).isFalse();

        // ReplayHeaderSanitizer error guards
        ReplayHeaderSanitizer sanitizer = new ReplayHeaderSanitizer(clock);
        assertThatThrownBy(() -> sanitizer.sanitizeAndStamp(Map.of(), planId, new MessageCoordinates("t", 0, 0), -1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sanitizer.sanitizeAndStamp(Map.of(), planId, new MessageCoordinates("t", 0, 0), 0, -1))
                .isInstanceOf(IllegalArgumentException.class);

        // Models null/validation branches
        assertThatThrownBy(() -> new MessageCoordinates(" ", 0, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MessageCoordinates("t", -1, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MessageCoordinates("t", 0, -1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ReplayMode.TargetTopic(" ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ReplayMode.CanaryTopic(" ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ReplayMode.PatchedPayload(" ", "{}")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ReplayMode.PatchedPayload("t", " ")).isInstanceOf(IllegalArgumentException.class);

        DltMessageSummary noPart = new DltMessageSummary(
                planId, "t.dlt", 0, 0L, null, null, null, null, -5, null, PayloadFormat.TOMBSTONE, null, 0,
                "fp", FailureCategory.UNKNOWN, 0, MessageStatus.NEW, null, 0L, null, null, null, null, now, now);
        assertThat(noPart.stripeId(4)).isEqualTo(3);
        assertThatThrownBy(() -> noPart.stripeId(0)).isInstanceOf(IllegalArgumentException.class);
        FingerprintCluster cl = new FingerprintCluster("fp", null, null, null, null, FailureCategory.UNKNOWN, "r", now, now, 0, 0);
        DltMessageDetail nullDetail = new DltMessageDetail(noPart, cl, null, null, "RAW", null, Map.of(), null, null, null);
        assertThat(nullDetail.messageKey()).isNull();
        assertThat(nullDetail.payload()).isNull();
        IngestedDltMessage nullIngested = new IngestedDltMessage(
                planId, "t.dlt", 0, 0L, null, null, null, null, null, 0, null, null,
                PayloadFormat.TOMBSTONE, null, 0, "fp", null, null, null, null,
                FailureCategory.UNKNOWN, "r", 0, MessageStatus.NEW, null, Map.of(), now);
        assertThat(nullIngested.messageKey()).isNull();
        assertThat(nullIngested.payload()).isNull();
    }
}
