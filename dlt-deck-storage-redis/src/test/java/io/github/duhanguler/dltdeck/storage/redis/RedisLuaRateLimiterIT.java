package io.github.duhanguler.dltdeck.storage.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class RedisLuaRateLimiterIT {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Test
    void enforcesGlobalClusterRateLimitAtomicallyViaLuaScript() {
        var config = new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getMappedPort(6379));
        LettuceConnectionFactory factory = new LettuceConnectionFactory(config);
        factory.afterPropertiesSet();
        try {
            StringRedisTemplate template = new StringRedisTemplate(factory);
            Instant[] current = {Instant.parse("2026-11-24T12:00:00Z")};
            Clock mutableClock = new Clock() {
                @Override
                public ZoneId getZone() {
                    return ZoneOffset.UTC;
                }

                @Override
                public Clock withZone(ZoneId zone) {
                    return this;
                }

                @Override
                public Instant instant() {
                    return current[0];
                }
            };

            RedisLuaRateLimitCoordinator podA = new RedisLuaRateLimitCoordinator(template, mutableClock);
            RedisLuaRateLimitCoordinator podB = new RedisLuaRateLimitCoordinator(template, mutableClock);
            UUID planId = UUID.randomUUID();

            // Rate limit = 3/sec (burst = 3) shared across podA and podB
            assertThat(podA.tryAcquire(planId, 3)).isTrue();
            assertThat(podB.tryAcquire(planId, 3)).isTrue();
            assertThat(podA.tryAcquire(planId, 3)).isTrue();
            var blocked = podB.acquireWithWaitHint(planId, 3, 1);
            assertThat(blocked.allowed()).isFalse();
            assertThat(blocked.waitDuration()).isGreaterThan(Duration.ZERO);

            // Advance clock by 1 second -> tokens refill atomically
            current[0] = current[0].plusSeconds(1);
            assertThat(podB.tryAcquire(planId, 3)).isTrue();
        } finally {
            factory.destroy();
        }
    }
}
