package io.github.duhanguler.dltdeck.core.model;

import java.time.Instant;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Narrow, cache-hot projection stored in {@code dlt_deck_messages} with zero large payloads or stacktraces.
 */
public record DltMessageSummary(
        UUID id,
        String dltTopic,
        int dltPartition,
        long dltOffset,
        @Nullable String originalTopic,
        @Nullable Integer originalPartition,
        @Nullable Long originalOffset,
        @Nullable String originalConsumerGroup,
        int messageKeyHash,
        @Nullable String keyFingerprint,
        PayloadFormat payloadFormat,
        @Nullable Integer schemaId,
        int payloadSizeBytes,
        String fingerprint,
        FailureCategory failureCategory,
        int replayHops,
        MessageStatus status,
        @Nullable UUID activePlanId,
        long fencingToken,
        @Nullable String leaseOwner,
        @Nullable Instant leaseExpiresAt,
        @Nullable Integer publishedPartition,
        @Nullable Long publishedOffset,
        Instant ingestedAt,
        Instant updatedAt) {

    public MessageCoordinates dltCoordinates() {
        return new MessageCoordinates(dltTopic, dltPartition, dltOffset);
    }

    public int stripeId(int stripeCount) {
        if (stripeCount <= 0) {
            throw new IllegalArgumentException("stripeCount must be positive");
        }
        if (originalPartition != null && originalPartition >= 0) {
            return Math.floorMod(originalPartition, stripeCount);
        }
        return Math.floorMod(messageKeyHash, stripeCount);
    }
}
