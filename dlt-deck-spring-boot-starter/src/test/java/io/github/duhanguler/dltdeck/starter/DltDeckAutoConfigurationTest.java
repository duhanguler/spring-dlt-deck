package io.github.duhanguler.dltdeck.starter;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.duhanguler.dltdeck.core.codec.PayloadCodec;
import io.github.duhanguler.dltdeck.core.model.PayloadFormat;
import io.github.duhanguler.dltdeck.core.spi.DltMessageRepository;
import io.github.duhanguler.dltdeck.core.spi.ReplayRateLimitCoordinator;
import io.github.duhanguler.dltdeck.starter.autoconfigure.DltDeckCoreAutoConfiguration;
import io.github.duhanguler.dltdeck.starter.autoconfigure.DltDeckJdbcStorageAutoConfiguration;
import io.github.duhanguler.dltdeck.starter.autoconfigure.DltDeckKafkaAutoConfiguration;
import io.github.duhanguler.dltdeck.starter.autoconfigure.DltDeckObservabilityAutoConfiguration;
import io.github.duhanguler.dltdeck.starter.autoconfigure.DltDeckSecurityAutoConfiguration;
import io.github.duhanguler.dltdeck.starter.autoconfigure.DltDeckWebAutoConfiguration;
import io.github.duhanguler.dltdeck.starter.service.DltDeckControlPlaneService;
import java.time.Duration;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

class DltDeckAutoConfigurationTest {

    private final WebApplicationContextRunner contextRunner = new WebApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    DltDeckCoreAutoConfiguration.class,
                    DltDeckJdbcStorageAutoConfiguration.class,
                    DltDeckKafkaAutoConfiguration.class,
                    DltDeckSecurityAutoConfiguration.class,
                    DltDeckWebAutoConfiguration.class,
                    DltDeckObservabilityAutoConfiguration.class))
            .withUserConfiguration(H2DataSourceConfig.class)
            .withPropertyValues(
                    "dlt.deck.ingestion.enabled=false",
                    "dlt.deck.kafka.bootstrap-servers=localhost:19092");

    @Test
    void backsOffCompletelyWhenDisabled() {
        contextRunner.withPropertyValues("dlt.deck.enabled=false").run(ctx -> {
            assertThat(ctx).doesNotHaveBean(DltDeckControlPlaneService.class);
            assertThat(ctx).doesNotHaveBean(DltMessageRepository.class);
            assertThat(ctx).doesNotHaveBean("dltDeckRouterFunction");
            assertThat(ctx).doesNotHaveBean("dltDeckJsonMapper");
            assertThat(ctx).doesNotHaveBean("dltDeckInternalKafkaTemplate");
        });
    }

    @Test
    void registersIsolatedBeansAndNeverRegistersGlobalWebMvcConfigurer() {
        contextRunner.run(ctx -> {
            assertThat(ctx).hasSingleBean(DltDeckControlPlaneService.class);
            assertThat(ctx).hasBean("dltDeckRouterFunction");
            assertThat(ctx).hasBean("dltDeckJsonMapper");
            assertThat(ctx).hasBean("dltDeckInternalKafkaTemplate");
            assertThat(ctx).doesNotHaveBean(WebMvcConfigurer.class);
        });
    }

    @Test
    void honorsCustomSpiBeanOverrides() {
        contextRunner.withUserConfiguration(CustomSpiConfig.class).run(ctx -> {
            assertThat(ctx.getBean(PayloadCodec.class)).isSameAs(CustomSpiConfig.CUSTOM_CODEC);
            assertThat(ctx.getBean(ReplayRateLimitCoordinator.class)).isSameAs(CustomSpiConfig.CUSTOM_LIMITER);
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class H2DataSourceConfig {
        @Bean(destroyMethod = "close")
        DataSource dataSource() {
            HikariConfig cfg = new HikariConfig();
            cfg.setJdbcUrl("jdbc:h2:mem:autoconf_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
            cfg.setUsername("sa");
            cfg.setPassword("");
            return new HikariDataSource(cfg);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomSpiConfig {
        static final PayloadCodec CUSTOM_CODEC = (payload, headers, hasDeser) -> new PayloadCodec.DecodedPayload(
                PayloadFormat.UTF8_TEXT, false, null, payload == null ? 0 : payload.length, "custom", null, null, null);
        static final ReplayRateLimitCoordinator CUSTOM_LIMITER = new ReplayRateLimitCoordinator() {
            @Override
            public boolean tryAcquire(UUID planId, int rateLimitPerSec) {
                return true;
            }

            @Override
            public Duration suggestedWait(UUID planId, int rateLimitPerSec) {
                return Duration.ZERO;
            }
        };

        @Bean
        PayloadCodec payloadCodec() {
            return CUSTOM_CODEC;
        }

        @Bean
        ReplayRateLimitCoordinator replayRateLimitCoordinator() {
            return CUSTOM_LIMITER;
        }
    }
}
