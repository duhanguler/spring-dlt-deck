package io.github.duhanguler.dltdeck.starter.autoconfigure;

import io.github.duhanguler.dltdeck.starter.kafka.DltBatchIngestionEngine;
import io.github.duhanguler.dltdeck.starter.observability.DltDeckActuatorEndpoint;
import io.github.duhanguler.dltdeck.starter.observability.DltDeckHealthIndicator;
import io.github.duhanguler.dltdeck.starter.observability.DltDeckMetricsBinder;
import io.github.duhanguler.dltdeck.starter.service.DltDeckControlPlaneService;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@AutoConfiguration(after = DltDeckKafkaAutoConfiguration.class)
@ConditionalOnProperty(prefix = "dlt.deck", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnBean(DltDeckControlPlaneService.class)
public class DltDeckObservabilityAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(Endpoint.class)
    static class ActuatorConfig {
        @Bean
        @ConditionalOnMissingBean
        DltDeckActuatorEndpoint dltDeckActuatorEndpoint(DltDeckControlPlaneService service) {
            return new DltDeckActuatorEndpoint(service);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(HealthIndicator.class)
    static class HealthConfig {
        @Bean(name = "dltDeckHealthIndicator")
        @ConditionalOnMissingBean(name = "dltDeckHealthIndicator")
        DltDeckHealthIndicator dltDeckHealthIndicator(DltDeckControlPlaneService service) {
            return new DltDeckHealthIndicator(service);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    static class MetricsConfig {
        @Bean
        @ConditionalOnMissingBean
        DltDeckMetricsBinder dltDeckMetricsBinder(
                DltBatchIngestionEngine ingestionEngine, DltDeckControlPlaneService service) {
            return new DltDeckMetricsBinder(ingestionEngine, service);
        }
    }
}
