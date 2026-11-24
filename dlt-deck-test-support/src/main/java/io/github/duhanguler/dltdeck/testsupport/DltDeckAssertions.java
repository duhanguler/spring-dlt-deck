package io.github.duhanguler.dltdeck.testsupport;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.duhanguler.dltdeck.core.model.AuditChainVerification;
import io.github.duhanguler.dltdeck.core.model.DltMessageSummary;
import io.github.duhanguler.dltdeck.core.model.FailureCategory;
import io.github.duhanguler.dltdeck.core.model.MessageStatus;
import io.github.duhanguler.dltdeck.core.spi.AuditLedgerRepository;
import io.github.duhanguler.dltdeck.core.spi.DltMessageRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fluent AssertJ CI guardrails for Spring Boot / Kafka integration pipelines using {@code spring-dlt-deck}.
 */
public final class DltDeckAssertions {

    private DltDeckAssertions() {}

    public static void assertNoPoisonPills(DltMessageRepository repository, String dltTopic) {
        List<DltMessageSummary> items =
                repository.listMessages(dltTopic, null, MessageStatus.NEW, null, 200).items();
        List<DltMessageSummary> poison = items.stream()
                .filter(m -> m.failureCategory() == FailureCategory.DESERIALIZATION
                        || m.failureCategory() == FailureCategory.POISON_DATA)
                .toList();
        assertThat(poison)
                .as("Expected zero poison-pill/deserialization DLT records in topic %s", dltTopic)
                .isEmpty();
    }

    public static void assertNoIndeterminateOrDuplicates(
            DltMessageRepository repository, String dltTopic, List<String> deliveredIdempotencyKeys) {
        List<DltMessageSummary> indeterminate =
                repository.listMessages(dltTopic, null, MessageStatus.INDETERMINATE, null, 200).items();
        assertThat(indeterminate)
                .as("Expected zero INDETERMINATE records in topic %s", dltTopic)
                .isEmpty();

        Set<String> unique = new HashSet<>(deliveredIdempotencyKeys);
        assertThat(unique)
                .as("Detected duplicate replayed deliveries (idempotency keys)")
                .hasSameSizeAs(deliveredIdempotencyKeys);
    }

    public static void assertKeyOrderPreserved(List<KeyedSequenceEvent> deliveredEvents) {
        Map<String, List<Long>> perKeySequences = new HashMap<>();
        for (KeyedSequenceEvent evt : deliveredEvents) {
            perKeySequences.computeIfAbsent(evt.key(), _ -> new ArrayList<>()).add(evt.sequence());
        }
        for (Map.Entry<String, List<Long>> entry : perKeySequences.entrySet()) {
            List<Long> seqs = entry.getValue();
            for (int i = 1; i < seqs.size(); i++) {
                assertThat(seqs.get(i))
                        .as("Out-of-order replay detected for key '%s': %s", entry.getKey(), seqs)
                        .isGreaterThan(seqs.get(i - 1));
            }
        }
    }

    public static void assertAuditChainIntact(AuditLedgerRepository auditRepository) {
        AuditChainVerification verification = auditRepository.verifyIntegrity();
        assertThat(verification.valid())
                .as(
                        "Audit ledger hash chain corrupted at sequence %s: %s",
                        verification.firstInvalidSequenceNo(), verification.failureReason())
                .isTrue();
    }

    public static void assertQuarantinedAfterMaxHops(DltMessageRepository repository, String dltTopic, int maxHops) {
        List<DltMessageSummary> all = repository.listMessages(dltTopic, null, null, null, 200).items();
        List<DltMessageSummary> unquarantinedLoops = all.stream()
                .filter(m -> m.replayHops() >= maxHops && m.status() != MessageStatus.QUARANTINED)
                .toList();
        assertThat(unquarantinedLoops)
                .as("Records with replayHops >= %d must be QUARANTINED", maxHops)
                .isEmpty();
    }

    public record KeyedSequenceEvent(String key, long sequence) {}
}
