package io.github.duhanguler.dltdeck.starter.observability;

import io.github.duhanguler.dltdeck.starter.kafka.DltBatchIngestionEngine;
import io.github.duhanguler.dltdeck.starter.service.DltDeckControlPlaneService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * Micrometer metrics binder exposing live DLT ingestion lag, ingested volume, indeterminate alerts,
 * and blast-radius circuit trips for Prometheus & Grafana dashboards.
 */
public final class DltDeckMetricsBinder implements MeterBinder {

    private final DltBatchIngestionEngine ingestionEngine;
    private final DltDeckControlPlaneService controlPlaneService;

    public DltDeckMetricsBinder(
            DltBatchIngestionEngine ingestionEngine, DltDeckControlPlaneService controlPlaneService) {
        this.ingestionEngine = ingestionEngine;
        this.controlPlaneService = controlPlaneService;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("dlt.deck.ingestion.lag", ingestionEngine, DltBatchIngestionEngine::totalIngestionLag)
                .description("Current total Kafka DLT ingestion lag across assigned partitions")
                .register(registry);
        Gauge.builder("dlt.deck.ingestion.ingested.total", ingestionEngine, DltBatchIngestionEngine::totalIngestedCount)
                .description("Total DLT records ingested by this pod")
                .register(registry);
        Gauge.builder("dlt.deck.messages.indeterminate", controlPlaneService, s -> s.overview().indeterminateCount())
                .description("Count of DLT messages in INDETERMINATE state awaiting manual review")
                .register(registry);
        Gauge.builder("dlt.deck.plans.blast_radius_paused", controlPlaneService, s -> s.overview().pausedBlastRadiusPlans())
                .description("Count of replay plans currently paused by blast-radius circuit breaker")
                .register(registry);
    }
}
