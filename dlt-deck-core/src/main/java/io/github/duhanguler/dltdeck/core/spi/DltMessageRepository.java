package io.github.duhanguler.dltdeck.core.spi;

import io.github.duhanguler.dltdeck.core.model.DltMessageDetail;
import io.github.duhanguler.dltdeck.core.model.DltMessageSummary;
import io.github.duhanguler.dltdeck.core.model.FingerprintCluster;
import io.github.duhanguler.dltdeck.core.model.IngestedDltMessage;
import io.github.duhanguler.dltdeck.core.model.KeysetPage;
import io.github.duhanguler.dltdeck.core.model.MessageStatus;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Hexagonal storage port for hot metadata, deduplicated fingerprints, and cold payload projections.
 */
public interface DltMessageRepository {

    int ingestBatch(List<IngestedDltMessage> messages);

    List<FingerprintCluster> listClusters(@Nullable String dltTopic, int limit);

    KeysetPage<DltMessageSummary> listMessages(
            @Nullable String dltTopic,
            @Nullable String fingerprint,
            @Nullable MessageStatus status,
            @Nullable String cursor,
            int limit);

    Optional<DltMessageDetail> findDetailById(UUID messageId);

    int countEligibleForPlan(@Nullable String dltTopicFilter, @Nullable String fingerprintFilter, int maxRecords);

    List<DltMessageSummary> findEligibleForStripe(
            @Nullable String dltTopicFilter,
            @Nullable String fingerprintFilter,
            int stripeCount,
            int stripeId,
            int limit);

    /**
     * Short TX1: atomically claims a single record for replay, incrementing {@code fencing_token} and setting lease expiry.
     */
    Optional<ClaimedMessage> claimForReplay(
            UUID messageId, UUID planId, String leaseOwner, Instant leaseExpiresAt, Instant now);

    /**
     * Short TX2: transitions a claimed record to {@code PUBLISHED}, strictly fenced by {@code fencing_token}.
     */
    boolean markPublished(
            UUID messageId,
            long expectedFencingToken,
            int publishedPartition,
            long publishedOffset,
            Instant now);

    /**
     * Short TX2 (error path): broker/serialization error known not to have committed on broker.
     */
    boolean markFailedPublish(UUID messageId, long expectedFencingToken, Instant now);

    /**
     * Marks remaining eligible messages for the same {@code keyFingerprint} as {@code BLOCKED_BY_PRIOR_KEY_FAILURE}
     * to prevent head-of-line state corruption, preserving the root failing record's status.
     */
    int blockSubsequentForKey(
            UUID planId, String dltTopic, String keyFingerprint, @Nullable UUID excludeMessageId, Instant now);

    /**
     * Transitions expired {@code CLAIMED} leases strictly to {@code INDETERMINATE} (never silently re-publishing).
     */
    List<DltMessageSummary> sweepExpiredLeasesToIndeterminate(Instant now, int limit);

    boolean resolveIndeterminate(UUID messageId, MessageStatus targetStatus, Instant now);

    boolean discardMessage(UUID messageId, Instant now);

    boolean markCorrelatedReDlt(String dltTopic, int dltPartition, long dltOffset, Instant now);

    int purgeOlderThan(Instant cutoff, int batchLimit);

    record ClaimedMessage(DltMessageDetail detail, long fencingToken) {}
}
