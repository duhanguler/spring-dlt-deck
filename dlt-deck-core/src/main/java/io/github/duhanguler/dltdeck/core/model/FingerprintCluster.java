package io.github.duhanguler.dltdeck.core.model;

import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * Deduplicated exception cluster stored once per fingerprint in {@code dlt_deck_fingerprints}.
 */
public record FingerprintCluster(
        String fingerprint,
        @Nullable String exceptionFqcn,
        @Nullable String rootCauseFqcn,
        @Nullable String sampleMessage,
        @Nullable String canonicalStacktrace,
        FailureCategory failureCategory,
        String recommendation,
        Instant firstSeenAt,
        Instant lastSeenAt,
        long totalCount,
        long newCount) {}
