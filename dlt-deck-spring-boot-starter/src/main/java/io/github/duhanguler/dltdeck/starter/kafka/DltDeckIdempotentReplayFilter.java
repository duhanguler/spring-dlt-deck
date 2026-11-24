package io.github.duhanguler.dltdeck.starter.kafka;

import io.github.duhanguler.dltdeck.core.policy.ReplayHeaderSanitizer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.jspecify.annotations.Nullable;

/**
 * Optional consumer-side deduplication helper enabling end-to-end effective exactly-once semantics
 * across {@code INDETERMINATE} crash recoveries by tracking {@code x-dlt-deck-idempotency-key}.
 */
public final class DltDeckIdempotentReplayFilter {

    private final IdempotencyKeyStore keyStore;

    public DltDeckIdempotentReplayFilter() {
        this(new BoundedInMemoryKeyStore(10_000));
    }

    public DltDeckIdempotentReplayFilter(IdempotencyKeyStore keyStore) {
        this.keyStore = keyStore;
    }

    public @Nullable String extractIdempotencyKey(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(ReplayHeaderSanitizer.HEADER_IDEMPOTENCY_KEY);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    /**
     * Returns {@code true} if the record should be processed, or {@code false} if its replay idempotency
     * key was already observed. Non-replayed records (without the header) always return {@code true}.
     */
    public boolean shouldProcess(ConsumerRecord<?, ?> record) {
        String key = extractIdempotencyKey(record);
        if (key == null || key.isBlank()) {
            return true;
        }
        return keyStore.markIfAbsent(key);
    }

    @FunctionalInterface
    public interface IdempotencyKeyStore {
        boolean markIfAbsent(String idempotencyKey);
    }

    public static final class BoundedInMemoryKeyStore implements IdempotencyKeyStore {
        private final Map<String, Boolean> lru;

        public BoundedInMemoryKeyStore(int maxEntries) {
            this.lru = Collections.synchronizedMap(new LinkedHashMap<>(128, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > maxEntries;
                }
            });
        }

        @Override
        public boolean markIfAbsent(String idempotencyKey) {
            return lru.putIfAbsent(idempotencyKey, Boolean.TRUE) == null;
        }
    }
}
