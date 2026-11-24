package io.github.duhanguler.dltdeck.testsupport;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.duhanguler.dltdeck.core.model.AuditChainVerification;
import io.github.duhanguler.dltdeck.core.model.AuditEntry;
import io.github.duhanguler.dltdeck.core.model.DltMessageDetail;
import io.github.duhanguler.dltdeck.core.model.DltMessageSummary;
import io.github.duhanguler.dltdeck.core.model.FailureCategory;
import io.github.duhanguler.dltdeck.core.model.FingerprintCluster;
import io.github.duhanguler.dltdeck.core.model.IngestedDltMessage;
import io.github.duhanguler.dltdeck.core.model.KeysetPage;
import io.github.duhanguler.dltdeck.core.model.MessageStatus;
import io.github.duhanguler.dltdeck.core.model.PayloadFormat;
import io.github.duhanguler.dltdeck.core.spi.AuditLedgerRepository;
import io.github.duhanguler.dltdeck.core.spi.DltMessageRepository;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

class DltDeckAssertionsTest {

    @Test
    void verifiesKeyOrderDuplicatesAuditAndQuarantineAssertions() {
        DltDeckAssertions.assertKeyOrderPreserved(List.of(
                new DltDeckAssertions.KeyedSequenceEvent("k1", 1),
                new DltDeckAssertions.KeyedSequenceEvent("k2", 1),
                new DltDeckAssertions.KeyedSequenceEvent("k1", 2),
                new DltDeckAssertions.KeyedSequenceEvent("k2", 2)));

        assertThatThrownBy(() -> DltDeckAssertions.assertKeyOrderPreserved(List.of(
                        new DltDeckAssertions.KeyedSequenceEvent("k1", 2),
                        new DltDeckAssertions.KeyedSequenceEvent("k1", 1))))
                .isInstanceOf(AssertionError.class);

        StubMsgRepo emptyRepo = new StubMsgRepo(List.of());
        DltDeckAssertions.assertNoPoisonPills(emptyRepo, "orders.dlt");
        DltDeckAssertions.assertNoIndeterminateOrDuplicates(emptyRepo, "orders.dlt", List.of("a", "b"));
        DltDeckAssertions.assertQuarantinedAfterMaxHops(emptyRepo, "orders.dlt", 3);

        assertThatThrownBy(() -> DltDeckAssertions.assertNoIndeterminateOrDuplicates(
                        emptyRepo, "orders.dlt", List.of("dup", "dup")))
                .isInstanceOf(AssertionError.class);

        Instant now = Instant.now();
        DltMessageSummary poison = new DltMessageSummary(
                UUID.randomUUID(), "orders.dlt", 0, 1L, "orders", 0, 1L, "cg", 0, "kfp",
                PayloadFormat.UTF8_TEXT, null, 5, "fp", FailureCategory.DESERIALIZATION,
                3, MessageStatus.NEW, null, 0L, null, null, null, null, now, now);
        StubMsgRepo badRepo = new StubMsgRepo(List.of(poison));
        assertThatThrownBy(() -> DltDeckAssertions.assertNoPoisonPills(badRepo, "orders.dlt"))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> DltDeckAssertions.assertQuarantinedAfterMaxHops(badRepo, "orders.dlt", 3))
                .isInstanceOf(AssertionError.class);

        DltDeckAssertions.assertAuditChainIntact(new StubAuditRepo(AuditChainVerification.intact(5, "abc")));
        assertThatThrownBy(() -> DltDeckAssertions.assertAuditChainIntact(
                        new StubAuditRepo(AuditChainVerification.broken(5, 3L, "tampered"))))
                .isInstanceOf(AssertionError.class);
    }

    private record StubMsgRepo(List<DltMessageSummary> rows) implements DltMessageRepository {
        @Override public int ingestBatch(List<IngestedDltMessage> messages) { return 0; }
        @Override public List<FingerprintCluster> listClusters(@Nullable String dltTopic, int limit) { return List.of(); }
        @Override public KeysetPage<DltMessageSummary> listMessages(@Nullable String dltTopic, @Nullable String fingerprint, @Nullable MessageStatus status, @Nullable String cursor, int limit) {
            List<DltMessageSummary> filtered = status == null ? rows : rows.stream().filter(r -> r.status() == status).toList();
            return new KeysetPage<>(filtered, null, false);
        }
        @Override public Optional<DltMessageDetail> findDetailById(UUID messageId) { return Optional.empty(); }
        @Override public int countEligibleForPlan(@Nullable String dltTopicFilter, @Nullable String fingerprintFilter, int maxRecords) { return 0; }
        @Override public List<DltMessageSummary> findEligibleForStripe(@Nullable String dltTopicFilter, @Nullable String fingerprintFilter, int stripeCount, int stripeId, int limit) { return List.of(); }
        @Override public Optional<ClaimedMessage> claimForReplay(UUID messageId, UUID planId, String leaseOwner, Instant leaseExpiresAt, Instant now) { return Optional.empty(); }
        @Override public boolean markPublished(UUID messageId, long expectedFencingToken, int publishedPartition, long publishedOffset, Instant now) { return false; }
        @Override public boolean markFailedPublish(UUID messageId, long expectedFencingToken, Instant now) { return false; }
        @Override public int blockSubsequentForKey(UUID planId, String dltTopic, String keyFingerprint, @Nullable UUID excludeMessageId, Instant now) { return 0; }
        @Override public List<DltMessageSummary> sweepExpiredLeasesToIndeterminate(Instant now, int limit) { return List.of(); }
        @Override public boolean resolveIndeterminate(UUID messageId, MessageStatus targetStatus, Instant now) { return false; }
        @Override public boolean discardMessage(UUID messageId, Instant now) { return false; }
        @Override public boolean markCorrelatedReDlt(String dltTopic, int dltPartition, long dltOffset, Instant now) { return false; }
        @Override public int purgeOlderThan(Instant cutoff, int batchLimit) { return 0; }
    }

    private record StubAuditRepo(AuditChainVerification verification) implements AuditLedgerRepository {
        @Override public AuditEntry append(UUID eventId, String actor, String actorRole, String action, String targetType, String targetId, @Nullable String merkleRoot, String detailsJson, Instant createdAt) { throw new UnsupportedOperationException(); }
        @Override public List<AuditEntry> listRecent(int limit) { return List.of(); }
        @Override public List<AuditEntry> findAllOrdered() { return List.of(); }
        @Override public AuditChainVerification verifyIntegrity() { return verification; }
    }
}
