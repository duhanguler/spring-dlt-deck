package io.github.duhanguler.dltdeck.core.model;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Write model emitted by the Kafka batch ingestion engine for atomic 3-table persistence.
 */
@SuppressWarnings("ArrayRecordComponent")
public record IngestedDltMessage(
        UUID id,
        String dltTopic,
        int dltPartition,
        long dltOffset,
        @Nullable String originalTopic,
        @Nullable Integer originalPartition,
        @Nullable Long originalOffset,
        @Nullable String originalConsumerGroup,
        byte @Nullable [] messageKey,
        int messageKeyHash,
        @Nullable String keyFingerprint,
        byte @Nullable [] payload,
        PayloadFormat payloadFormat,
        @Nullable Integer schemaId,
        int payloadSizeBytes,
        String fingerprint,
        @Nullable String exceptionFqcn,
        @Nullable String rootCauseFqcn,
        @Nullable String sampleMessage,
        @Nullable String canonicalStacktrace,
        FailureCategory failureCategory,
        String recommendation,
        int replayHops,
        MessageStatus initialStatus,
        @Nullable UUID correlatedPlanId,
        Map<String, String> headers,
        Instant ingestedAt) {

    public IngestedDltMessage(
            UUID id,
            String dltTopic,
            int dltPartition,
            long dltOffset,
            @Nullable String originalTopic,
            @Nullable Integer originalPartition,
            @Nullable Long originalOffset,
            @Nullable String originalConsumerGroup,
            byte @Nullable [] messageKey,
            int messageKeyHash,
            @Nullable String keyFingerprint,
            byte @Nullable [] payload,
            PayloadFormat payloadFormat,
            @Nullable Integer schemaId,
            int payloadSizeBytes,
            String fingerprint,
            @Nullable String exceptionFqcn,
            @Nullable String rootCauseFqcn,
            @Nullable String sampleMessage,
            @Nullable String canonicalStacktrace,
            FailureCategory failureCategory,
            String recommendation,
            int replayHops,
            MessageStatus initialStatus,
            @Nullable UUID correlatedPlanId,
            Map<String, String> headers,
            Instant ingestedAt) {
        this.id = id;
        this.dltTopic = dltTopic;
        this.dltPartition = dltPartition;
        this.dltOffset = dltOffset;
        this.originalTopic = originalTopic;
        this.originalPartition = originalPartition;
        this.originalOffset = originalOffset;
        this.originalConsumerGroup = originalConsumerGroup;
        this.messageKey = messageKey == null ? null : Arrays.copyOf(messageKey, messageKey.length);
        this.messageKeyHash = messageKeyHash;
        this.keyFingerprint = keyFingerprint;
        this.payload = payload == null ? null : Arrays.copyOf(payload, payload.length);
        this.payloadFormat = payloadFormat;
        this.schemaId = schemaId;
        this.payloadSizeBytes = payloadSizeBytes;
        this.fingerprint = fingerprint;
        this.exceptionFqcn = exceptionFqcn;
        this.rootCauseFqcn = rootCauseFqcn;
        this.sampleMessage = sampleMessage;
        this.canonicalStacktrace = canonicalStacktrace;
        this.failureCategory = failureCategory;
        this.recommendation = recommendation;
        this.replayHops = replayHops;
        this.initialStatus = initialStatus;
        this.correlatedPlanId = correlatedPlanId;
        this.headers = Map.copyOf(headers);
        this.ingestedAt = ingestedAt;
    }

    @Override
    public byte @Nullable [] messageKey() {
        return messageKey == null ? null : Arrays.copyOf(messageKey, messageKey.length);
    }

    @Override
    public byte @Nullable [] payload() {
        return payload == null ? null : Arrays.copyOf(payload, payload.length);
    }
}
