package io.github.duhanguler.dltdeck.core.spi;

import io.github.duhanguler.dltdeck.core.model.PlanStripeLease;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Deterministic multi-pod stripe lease coordinator guaranteeing at most one active pod per stripe.
 */
public interface StripeLeaseStore {

    void initializeStripes(UUID planId, int stripeCount);

    Optional<PlanStripeLease> acquireNextStripe(
            UUID planId, String leaseOwner, Instant leaseExpiresAt, Instant now);

    boolean renewLease(
            UUID planId, int stripeId, String leaseOwner, long fencingToken, Instant newExpiry);

    void markCompleted(UUID planId, int stripeId, String leaseOwner, long fencingToken);

    void pauseAllStripes(UUID planId);

    List<PlanStripeLease> findStripes(UUID planId);
}
