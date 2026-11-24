package io.github.duhanguler.dltdeck.starter.kafka;

import io.github.duhanguler.dltdeck.core.codec.PayloadCodec;
import io.github.duhanguler.dltdeck.core.exception.NotFoundException;
import io.github.duhanguler.dltdeck.core.exception.ValidationDomainException;
import io.github.duhanguler.dltdeck.core.model.DltMessageDetail;
import io.github.duhanguler.dltdeck.core.model.DryRunOutcome;
import io.github.duhanguler.dltdeck.core.model.DryRunTier;
import io.github.duhanguler.dltdeck.core.model.PayloadFormat;
import io.github.duhanguler.dltdeck.core.patch.JsonPayloadPatcher;
import io.github.duhanguler.dltdeck.core.policy.ReplayHeaderSanitizer;
import io.github.duhanguler.dltdeck.core.spi.DltDryRunHandler;
import io.github.duhanguler.dltdeck.core.spi.DltMessageRepository;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.jspecify.annotations.Nullable;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Honest 3-Tier Dry-Run engine:
 * <ul>
 *   <li><b>Tier 1 (Always Side-Effect Free):</b> optional RFC 7396 merge patch + JSON parse + format/schema checks + optional Jakarta Bean Validation.</li>
 *   <li><b>Tier 2 (Explicit Opt-In SPI):</b> executes user-registered {@link DltDryRunHandler} beans with Virtual-Thread timeout isolation.</li>
 *   <li><b>Tier 3 (Canary Replay):</b> publishes a single sanitized record to a validated {@code *.canary} topic.</li>
 * </ul>
 */
public final class TieredDryRunService {

    private final DltMessageRepository messageRepository;
    private final JsonPayloadPatcher payloadPatcher;
    private final PayloadCodec payloadCodec;
    private final JsonMapper jsonMapper;
    private final @Nullable Validator validator;
    private final List<DltDryRunHandler> dryRunHandlers;
    private final TargetTopicValidator targetTopicValidator;
    private final ReplayHeaderSanitizer headerSanitizer;
    private final KafkaTemplate<byte[], byte[]> kafkaTemplate;

    public TieredDryRunService(
            DltMessageRepository messageRepository,
            JsonPayloadPatcher payloadPatcher,
            PayloadCodec payloadCodec,
            JsonMapper jsonMapper,
            @Nullable Validator validator,
            List<DltDryRunHandler> dryRunHandlers,
            TargetTopicValidator targetTopicValidator,
            ReplayHeaderSanitizer headerSanitizer,
            KafkaTemplate<byte[], byte[]> kafkaTemplate) {
        this.messageRepository = messageRepository;
        this.payloadPatcher = payloadPatcher;
        this.payloadCodec = payloadCodec;
        this.jsonMapper = jsonMapper;
        this.validator = validator;
        this.dryRunHandlers = List.copyOf(dryRunHandlers);
        this.targetTopicValidator = targetTopicValidator;
        this.headerSanitizer = headerSanitizer;
        this.kafkaTemplate = kafkaTemplate;
    }

    public DryRunOutcome executeDryRun(
            UUID messageId,
            DryRunTier tier,
            @Nullable String targetTopic,
            @Nullable String mergePatchJson,
            @Nullable Class<?> validationTargetClass) {
        long startMs = System.currentTimeMillis();
        DltMessageDetail detail = messageRepository
                .findDetailById(messageId)
                .orElseThrow(() -> new NotFoundException("MESSAGE_NOT_FOUND", "Message not found: " + messageId));

        byte[] candidateBytes = detail.payload();
        List<String> diagnostics = new ArrayList<>();

        if (mergePatchJson != null && !mergePatchJson.isBlank()) {
            try {
                JsonPayloadPatcher.PatchResult patchResult = payloadPatcher.applyMergePatch(
                        detail.summary().payloadFormat(), candidateBytes, mergePatchJson);
                candidateBytes = patchResult.patchedBytes();
                diagnostics.add("RFC 7396 merge patch applied cleanly (" + patchResult.diffs().size() + " field changes)");
            } catch (ValidationDomainException ex) {
                diagnostics.add("Patch rejected [" + ex.errorCode() + "]: " + ex.getMessage());
                return new DryRunOutcome(
                        tier, false, "Patch validation failed: " + ex.errorCode(), diagnostics, null, null, null, elapsed(startMs));
            }
        }

        return switch (tier) {
            case TIER_1_VALIDATE -> runTier1(detail, candidateBytes, validationTargetClass, diagnostics, startMs);
            case TIER_2_HANDLER_SPI -> runTier2(detail, targetTopic, candidateBytes, diagnostics, startMs);
            case TIER_3_CANARY -> runTier3Canary(detail, targetTopic, candidateBytes, diagnostics, startMs);
        };
    }

    private DryRunOutcome runTier1(
            DltMessageDetail detail,
            byte @Nullable [] candidateBytes,
            @Nullable Class<?> targetClass,
            List<String> diagnostics,
            long startMs) {
        PayloadCodec.DecodedPayload decoded = payloadCodec.decode(candidateBytes, detail.headers(), false);
        diagnostics.add("Detected format: " + decoded.format() + " (" + decoded.sizeBytes() + " bytes, patchable=" + decoded.patchable() + ")");
        if (decoded.schemaId() != null) {
            diagnostics.add("Confluent Schema Registry ID: " + decoded.schemaId());
        }

        if (decoded.format() == PayloadFormat.JSON && candidateBytes != null) {
            try {
                JsonNode tree = jsonMapper.readTree(candidateBytes);
                diagnostics.add("JSON syntax verified cleanly (" + tree.size() + " top-level elements)");
                if (targetClass != null) {
                    Object bound = jsonMapper.treeToValue(tree, targetClass);
                    if (validator != null && bound != null) {
                        Set<ConstraintViolation<Object>> violations = validator.validate(bound);
                        if (!violations.isEmpty()) {
                            for (ConstraintViolation<Object> v : violations) {
                                diagnostics.add("Constraint violation on '" + v.getPropertyPath() + "': " + v.getMessage());
                            }
                            return new DryRunOutcome(
                                    DryRunTier.TIER_1_VALIDATE,
                                    false,
                                    "Bean validation failed with " + violations.size() + " violations",
                                    diagnostics,
                                    null,
                                    null,
                                    null,
                                    elapsed(startMs));
                        }
                        diagnostics.add("Jakarta Bean Validation passed for " + targetClass.getSimpleName());
                    }
                }
            } catch (JacksonException ex) {
                diagnostics.add("Jackson 3 binding failed: " + ex.getOriginalMessage());
                return new DryRunOutcome(
                        DryRunTier.TIER_1_VALIDATE,
                        false,
                        "Deserialization/binding failed",
                        diagnostics,
                        null,
                        null,
                        null,
                        elapsed(startMs));
            }
        } else if (decoded.format() == PayloadFormat.UTF8_TEXT && detail.summary().failureCategory()
                == io.github.duhanguler.dltdeck.core.model.FailureCategory.DESERIALIZATION) {
            diagnostics.add("Payload failed JSON parsing and remains invalid text for JSON deserializers");
            return new DryRunOutcome(
                    DryRunTier.TIER_1_VALIDATE,
                    false,
                    "Malformed payload did not parse as valid JSON",
                    diagnostics,
                    null,
                    null,
                    null,
                    elapsed(startMs));
        }

        return new DryRunOutcome(
                DryRunTier.TIER_1_VALIDATE,
                true,
                "Tier-1 side-effect-free validation passed",
                diagnostics,
                null,
                null,
                null,
                elapsed(startMs));
    }

    private DryRunOutcome runTier2(
            DltMessageDetail detail,
            @Nullable String targetTopic,
            byte @Nullable [] candidateBytes,
            List<String> diagnostics,
            long startMs) {
        String topic = targetTopic != null && !targetTopic.isBlank()
                ? targetTopic
                : (detail.summary().originalTopic() != null ? detail.summary().originalTopic() : detail.summary().dltTopic());

        List<DltDryRunHandler> matching = dryRunHandlers.stream().filter(h -> h.supports(topic)).toList();
        if (matching.isEmpty()) {
            diagnostics.add("No DltDryRunHandler SPI bean registered for topic '" + topic + "'");
            return new DryRunOutcome(
                    DryRunTier.TIER_2_HANDLER_SPI,
                    false,
                    "No Tier-2 DltDryRunHandler registered for target topic",
                    diagnostics,
                    null,
                    null,
                    null,
                    elapsed(startMs));
        }

        byte[] payloadCopy = candidateBytes == null ? null : candidateBytes.clone();
        boolean allPassed = true;
        try (ExecutorService vexec = Executors.newVirtualThreadPerTaskExecutor()) {
            for (DltDryRunHandler handler : matching) {
                DltDryRunHandler.HandlerResult res = vexec
                        .submit(() -> handler.dryRun(topic, detail, payloadCopy))
                        .get(3, TimeUnit.SECONDS);
                diagnostics.add("[" + handler.getClass().getSimpleName() + "] " + res.summary());
                diagnostics.addAll(res.diagnostics());
                if (!res.passed()) {
                    allPassed = false;
                }
            }
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            diagnostics.add("Tier-2 handler execution error/timeout: " + ex.getMessage());
            return new DryRunOutcome(
                    DryRunTier.TIER_2_HANDLER_SPI,
                    false,
                    "Tier-2 handler failed or timed out",
                    diagnostics,
                    null,
                    null,
                    null,
                    elapsed(startMs));
        }
        return new DryRunOutcome(
                DryRunTier.TIER_2_HANDLER_SPI,
                allPassed,
                allPassed ? "Tier-2 domain dry-run handler(s) passed" : "Tier-2 domain dry-run handler rejected payload",
                diagnostics,
                null,
                null,
                null,
                elapsed(startMs));
    }

    private DryRunOutcome runTier3Canary(
            DltMessageDetail detail,
            @Nullable String canaryTopic,
            byte @Nullable [] candidateBytes,
            List<String> diagnostics,
            long startMs) {
        if (canaryTopic == null || canaryTopic.isBlank()) {
            throw new ValidationDomainException("CANARY_TOPIC_REQUIRED", "Tier-3 dry-run requires an explicit *.canary target topic");
        }
        targetTopicValidator.validateCanaryTarget(canaryTopic);

        Map<String, String> headers = headerSanitizer.sanitizeAndStamp(
                detail.headers(),
                UUID.randomUUID(),
                detail.summary().dltCoordinates(),
                detail.summary().fencingToken(),
                detail.summary().replayHops());
        headers.put("x-dlt-deck-canary", "true");

        ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(canaryTopic, detail.messageKey(), candidateBytes);
        for (Map.Entry<String, String> h : headers.entrySet()) {
            record.headers().add(new RecordHeader(h.getKey(), h.getValue().getBytes(StandardCharsets.UTF_8)));
        }
        try {
            RecordMetadata md = kafkaTemplate.send(record).get(10, TimeUnit.SECONDS).getRecordMetadata();
            diagnostics.add("Published canary record to " + canaryTopic + "[" + md.partition() + "]@" + md.offset());
            return new DryRunOutcome(
                    DryRunTier.TIER_3_CANARY,
                    true,
                    "Tier-3 canary record published",
                    diagnostics,
                    canaryTopic,
                    md.partition(),
                    md.offset(),
                    elapsed(startMs));
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            diagnostics.add("Canary publish failed: " + ex.getMessage());
            return new DryRunOutcome(
                    DryRunTier.TIER_3_CANARY,
                    false,
                    "Tier-3 canary publish failed",
                    diagnostics,
                    canaryTopic,
                    null,
                    null,
                    elapsed(startMs));
        }
    }

    private static long elapsed(long startMs) {
        return Math.max(0L, System.currentTimeMillis() - startMs);
    }
}
