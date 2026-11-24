package io.github.duhanguler.dltdeck.core.model;

import java.time.Instant;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Deterministic multi-pod stripe lease ensuring at most one worker pod processes stripe {@code stripeId}.
 */
public record PlanStripeLease(
        UUID planId,
        int stripeId,
        @Nullable String leaseOwner,
        @Nullable Instant leaseExpiresAt,
        long fencingToken,
        StripeStatus status) {

    public enum StripeStatus {
        PENDING,
        ACTIVE,
        COMPLETED,
        PAUSED
    }
}
