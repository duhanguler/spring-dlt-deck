package io.github.duhanguler.dltdeck.starter.service;

import io.github.duhanguler.dltdeck.core.policy.TokenBucketRateLimiter;
import io.github.duhanguler.dltdeck.core.spi.ReplayRateLimitCoordinator;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default in-JVM Virtual-Thread-friendly token-bucket rate limiter used when Redis is not configured.
 */
public final class LocalRateLimitCoordinator implements ReplayRateLimitCoordinator {

    private final Clock clock;
    private final Map<UUID, TokenBucketRateLimiter> buckets = new ConcurrentHashMap<>();

    public LocalRateLimitCoordinator(Clock clock) {
        this.clock = clock;
    }

    @Override
    public boolean tryAcquire(UUID planId, int rateLimitPerSec) {
        int safeRate = Math.max(1, rateLimitPerSec);
        TokenBucketRateLimiter limiter =
                buckets.computeIfAbsent(planId, _ -> new TokenBucketRateLimiter(clock, safeRate, safeRate));
        return limiter.tryAcquire();
    }

    @Override
    public Duration suggestedWait(UUID planId, int rateLimitPerSec) {
        int safeRate = Math.max(1, rateLimitPerSec);
        TokenBucketRateLimiter limiter =
                buckets.computeIfAbsent(planId, _ -> new TokenBucketRateLimiter(clock, safeRate, safeRate));
        return limiter.delayUntilAvailable(1);
    }
}
