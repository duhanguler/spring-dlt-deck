package io.github.duhanguler.dltdeck.core.spi;

import io.github.duhanguler.dltdeck.core.model.ReplayPlan;
import io.github.duhanguler.dltdeck.core.model.ReplayPlanStatus;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

public interface ReplayPlanRepository {

    ReplayPlan save(ReplayPlan plan);

    Optional<ReplayPlan> findById(UUID planId);

    List<ReplayPlan> listRecent(int limit);

    List<ReplayPlan> findByStatus(ReplayPlanStatus status);

    boolean approve(UUID planId, String approvedBy, Instant approvedAt);

    boolean reject(UUID planId, String rejectedBy, String reason, Instant rejectedAt);

    boolean updateStatus(UUID planId, ReplayPlanStatus expectedCurrent, ReplayPlanStatus newStatus, @Nullable Instant completedAt);

    void incrementCounters(
            UUID planId,
            int claimedDelta,
            int publishedDelta,
            int failedDelta,
            int indeterminateDelta,
            int blockedHolDelta,
            int reDltDelta);
}
