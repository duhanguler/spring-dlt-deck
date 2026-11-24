package io.github.duhanguler.dltdeck.core.model;

import java.time.Instant;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

public record AuditEntry(
        long sequenceNo,
        UUID eventId,
        String actor,
        String actorRole,
        String action,
        String targetType,
        String targetId,
        @Nullable String merkleRoot,
        String detailsJson,
        String prevHash,
        String entryHash,
        Instant createdAt) {}
