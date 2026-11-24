package io.github.duhanguler.dltdeck.starter.observability;

import io.github.duhanguler.dltdeck.starter.service.DltDeckControlPlaneService;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * Surfaces live DLT ingestion lag, subscribed topics, indeterminate alerts, and blast-radius circuit trips.
 */
public final class DltDeckHealthIndicator implements HealthIndicator {

    private final DltDeckControlPlaneService controlPlaneService;

    public DltDeckHealthIndicator(DltDeckControlPlaneService controlPlaneService) {
        this.controlPlaneService = controlPlaneService;
    }

    @Override
    public Health health() {
        var overview = controlPlaneService.overview();
        Health.Builder builder = overview.pausedBlastRadiusPlans() > 0
                ? Health.status("DEGRADED")
                : Health.up();
        return builder.withDetail("ingestionRunning", overview.ingestionRunning())
                .withDetail("totalIngestionLag", overview.totalIngestionLag())
                .withDetail("subscribedTopics", overview.subscribedTopics())
                .withDetail("indeterminateCount", overview.indeterminateCount())
                .withDetail("blockedHolCount", overview.blockedHolCount())
                .withDetail("pausedBlastRadiusPlans", overview.pausedBlastRadiusPlans())
                .build();
    }
}
