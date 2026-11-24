package io.github.duhanguler.dltdeck.core.spi;

import java.time.Duration;
import java.util.UUID;

/**
 * Cluster-wide or local replay rate limiter SPI (implemented locally in core/starter and globally via Redis Lua).
 */
public interface ReplayRateLimitCoordinator {

    boolean tryAcquire(UUID planId, int rateLimitPerSec);

    Duration suggestedWait(UUID planId, int rateLimitPerSec);
}
