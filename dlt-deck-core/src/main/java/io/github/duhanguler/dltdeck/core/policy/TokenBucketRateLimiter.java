package io.github.duhanguler.dltdeck.core.policy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Clock-injectable, Virtual-Thread-friendly (ReentrantLock, zero synchronized pinning) token-bucket rate limiter.
 */
public final class TokenBucketRateLimiter {

    private final Clock clock;
    private final double permitsPerSecond;
    private final double maxBurst;
    private final ReentrantLock lock = new ReentrantLock();

    private double availableTokens;
    private Instant lastRefillAt;

    public TokenBucketRateLimiter(Clock clock, double permitsPerSecond, double maxBurst) {
        if (permitsPerSecond <= 0.0) {
            throw new IllegalArgumentException("permitsPerSecond must be positive");
        }
        if (maxBurst < 1.0) {
            throw new IllegalArgumentException("maxBurst must be >= 1.0");
        }
        this.clock = clock;
        this.permitsPerSecond = permitsPerSecond;
        this.maxBurst = maxBurst;
        this.availableTokens = maxBurst;
        this.lastRefillAt = Instant.now(clock);
    }

    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    public boolean tryAcquire(int permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("permits must be positive");
        }
        lock.lock();
        try {
            refillLocked();
            if (availableTokens >= permits) {
                availableTokens -= permits;
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Computes how long a caller would need to wait for {@code permits} to become available without consuming them.
     */
    public Duration delayUntilAvailable(int permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("permits must be positive");
        }
        lock.lock();
        try {
            refillLocked();
            if (availableTokens >= permits) {
                return Duration.ZERO;
            }
            double deficit = permits - availableTokens;
            long nanos = (long) Math.ceil((deficit / permitsPerSecond) * 1_000_000_000.0);
            return Duration.ofNanos(Math.max(1L, nanos));
        } finally {
            lock.unlock();
        }
    }

    public double availableTokens() {
        lock.lock();
        try {
            refillLocked();
            return availableTokens;
        } finally {
            lock.unlock();
        }
    }

    private void refillLocked() {
        Instant now = Instant.now(clock);
        Duration elapsed = Duration.between(lastRefillAt, now);
        if (!elapsed.isNegative() && !elapsed.isZero()) {
            double seconds = elapsed.toNanos() / 1_000_000_000.0;
            availableTokens = Math.min(maxBurst, availableTokens + (seconds * permitsPerSecond));
            lastRefillAt = now;
        }
    }
}
