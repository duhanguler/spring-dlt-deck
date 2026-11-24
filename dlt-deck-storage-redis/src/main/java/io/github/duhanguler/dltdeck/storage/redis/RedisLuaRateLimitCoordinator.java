package io.github.duhanguler.dltdeck.storage.redis;

import io.github.duhanguler.dltdeck.core.spi.ReplayRateLimitCoordinator;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * Atomic Lua-scripted cluster-wide token-bucket rate limiter backed by Redis.
 * Ensures multi-pod {@code spring-dlt-deck} clusters honor exact global {@code rateLimitPerSec}.
 */
public final class RedisLuaRateLimitCoordinator implements ReplayRateLimitCoordinator {

    private static final String KEY_PREFIX = "dltdeck:ratelimit:";
    private static final int TTL_SECONDS = 300;

    private final StringRedisTemplate redisTemplate;
    private final Clock clock;
    private final DefaultRedisScript<List> tokenBucketScript;

    public RedisLuaRateLimitCoordinator(StringRedisTemplate redisTemplate, Clock clock) {
        this.redisTemplate = redisTemplate;
        this.clock = clock;
        this.tokenBucketScript = new DefaultRedisScript<>();
        this.tokenBucketScript.setLocation(new ClassPathResource("dltdeck/redis/token_bucket.lua"));
        this.tokenBucketScript.setResultType(List.class);
    }

    @Override
    public boolean tryAcquire(UUID planId, int rateLimitPerSec) {
        List<?> result = executeBucket(planId, rateLimitPerSec, 1);
        if (result == null || result.isEmpty()) {
            return false;
        }
        return toLong(result.getFirst()) == 1L;
    }

    @Override
    public Duration suggestedWait(UUID planId, int rateLimitPerSec) {
        long ms = Math.max(10L, 1000L / Math.max(1, rateLimitPerSec));
        return Duration.ofMillis(ms);
    }

    public RateLimitResult acquireWithWaitHint(UUID planId, int rateLimitPerSec, int permits) {
        List<?> result = executeBucket(planId, rateLimitPerSec, permits);
        if (result == null || result.size() < 2) {
            return new RateLimitResult(false, suggestedWait(planId, rateLimitPerSec));
        }
        boolean allowed = toLong(result.get(0)) == 1L;
        long waitMs = Math.max(0L, toLong(result.get(1)));
        return new RateLimitResult(allowed, Duration.ofMillis(waitMs));
    }

    private @org.jspecify.annotations.Nullable List<?> executeBucket(
            UUID planId, int rateLimitPerSec, int requestedPermits) {
        int safeRate = Math.max(1, rateLimitPerSec);
        int burst = Math.max(1, safeRate);
        long nowMs = Instant.now(clock).toEpochMilli();
        String key = KEY_PREFIX + planId;
        return redisTemplate.execute(
                tokenBucketScript,
                List.of(key),
                Integer.toString(safeRate),
                Integer.toString(burst),
                Long.toString(nowMs),
                Integer.toString(Math.max(1, requestedPermits)),
                Integer.toString(TTL_SECONDS));
    }

    private static long toLong(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    public record RateLimitResult(boolean allowed, Duration waitDuration) {}
}
