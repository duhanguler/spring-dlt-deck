package io.github.duhanguler.dltdeck.core.spi;

import io.github.duhanguler.dltdeck.core.model.AuditChainVerification;
import io.github.duhanguler.dltdeck.core.model.AuditEntry;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

public interface AuditLedgerRepository {

    AuditEntry append(
            UUID eventId,
            String actor,
            String actorRole,
            String action,
            String targetType,
            String targetId,
            @Nullable String merkleRoot,
            String detailsJson,
            Instant createdAt);

    List<AuditEntry> listRecent(int limit);

    List<AuditEntry> findAllOrdered();

    AuditChainVerification verifyIntegrity();
}
