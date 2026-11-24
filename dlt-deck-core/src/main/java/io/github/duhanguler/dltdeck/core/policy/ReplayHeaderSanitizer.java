package io.github.duhanguler.dltdeck.core.policy;

import io.github.duhanguler.dltdeck.core.fingerprint.ExceptionFingerprinter;
import io.github.duhanguler.dltdeck.core.model.MessageCoordinates;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Strips Spring Kafka DLT/retry/deserializer exception headers, preserves business & W3C trace headers,
 * and stamps deterministic {@code x-dlt-deck-*} replay lineage and idempotency headers.
 */
public final class ReplayHeaderSanitizer {

    public static final String HEADER_IDEMPOTENCY_KEY = "x-dlt-deck-idempotency-key";
    public static final String HEADER_FENCING_TOKEN = "x-dlt-deck-fencing-token";
    public static final String HEADER_REPLAY_PLAN_ID = "x-dlt-deck-replay-plan-id";
    public static final String HEADER_REPLAY_HOPS = "x-dlt-deck-replay-hops";
    public static final String HEADER_ORIGIN_COORDS = "x-dlt-deck-origin-coords";
    public static final String HEADER_REPLAYED_AT = "x-dlt-deck-replayed-at";

    private static final List<String> STRIPPED_PREFIXES = List.of(
            "kafka_dlt-",
            "retry_topic-",
            "kafka_retry-",
            "springdeserializerexception",
            "dlt_",
            "x-dlt-deck-");

    private final Clock clock;

    public ReplayHeaderSanitizer(Clock clock) {
        this.clock = clock;
    }

    public Map<String, String> sanitizeAndStamp(
            Map<String, String> incomingHeaders,
            UUID planId,
            MessageCoordinates dltCoordinates,
            long fencingToken,
            int previousReplayHops) {
        if (fencingToken < 0) {
            throw new IllegalArgumentException("fencingToken must be >= 0");
        }
        if (previousReplayHops < 0) {
            throw new IllegalArgumentException("previousReplayHops must be >= 0");
        }

        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : incomingHeaders.entrySet()) {
            String key = entry.getKey();
            if (!shouldStrip(key)) {
                out.put(key, entry.getValue());
            }
        }

        out.put(HEADER_IDEMPOTENCY_KEY, computeIdempotencyKey(planId, dltCoordinates));
        out.put(HEADER_FENCING_TOKEN, Long.toString(fencingToken));
        out.put(HEADER_REPLAY_PLAN_ID, planId.toString());
        out.put(HEADER_REPLAY_HOPS, Integer.toString(previousReplayHops + 1));
        out.put(HEADER_ORIGIN_COORDS, dltCoordinates.format());
        out.put(HEADER_REPLAYED_AT, Instant.now(clock).toString());
        return out;
    }

    public static String computeIdempotencyKey(UUID planId, MessageCoordinates dltCoordinates) {
        String raw = planId + "|" + dltCoordinates.topic() + "|" + dltCoordinates.partition() + "|"
                + dltCoordinates.offset();
        return ExceptionFingerprinter.sha256Hex(raw);
    }

    public static boolean shouldStrip(String headerName) {
        String lower = headerName.toLowerCase(Locale.ROOT);
        for (String prefix : STRIPPED_PREFIXES) {
            if (lower.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
