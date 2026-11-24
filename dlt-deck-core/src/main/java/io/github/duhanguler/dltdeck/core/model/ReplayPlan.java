package io.github.duhanguler.dltdeck.core.model;

import java.time.Instant;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Governance & execution plan for DLT replay, bound to an immutable {@code planSpecHash} for TOCTOU defense.
 */
public record ReplayPlan(
        UUID id,
        String name,
        @Nullable String fingerprintFilter,
        @Nullable String dltTopicFilter,
        String targetTopic,
        ReplayOrderingMode orderingMode,
        @Nullable String payloadPatchJson,
        int rateLimitPerSec,
        int maxRecords,
        int snapshotCount,
        String planSpecHash,
        ReplayPlanStatus status,
        boolean requiresApproval,
        String createdBy,
        @Nullable String approvedBy,
        @Nullable String rejectionReason,
        int totalClaimed,
        int totalPublished,
        int totalFailed,
        int totalIndeterminate,
        int totalBlockedHol,
        int totalReDlt,
        Instant createdAt,
        @Nullable Instant approvedAt,
        @Nullable Instant completedAt) {}
