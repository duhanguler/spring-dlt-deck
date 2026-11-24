package io.github.duhanguler.dltdeck.starter.kafka;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;

/**
 * Safely decodes all Spring Kafka {@code KafkaHeaders.DLT_*} headers (binary big-endian and string forms)
 * and extracts raw poison-pill bytes from {@code springDeserializerExceptionValue} / {@code springDeserializerExceptionKey}
 * using a strict JEP 290/415 {@link ObjectInputFilter} allowlist (zero gadget deserialization risk).
 */
public final class SafeSpringHeaderExtractor {

    public static final String VALUE_DESER_EX_HEADER = "springDeserializerExceptionValue";
    public static final String KEY_DESER_EX_HEADER = "springDeserializerExceptionKey";

    private static final Logger log = LoggerFactory.getLogger(SafeSpringHeaderExtractor.class);

    /**
     * Strict allowlist for deserializing Spring Kafka {@link DeserializationException} headers.
     * Blocks every class outside this allowlist ({@code !*}) and caps object graph depth/refs/bytes.
     */
    private static final ObjectInputFilter STRICT_DESER_EXCEPTION_FILTER = ObjectInputFilter.Config.createFilter(
            "maxdepth=16;maxrefs=4096;maxbytes=1048576;"
                    + "org.springframework.kafka.support.serializer.DeserializationException;"
                    + "org.springframework.kafka.KafkaException;"
                    + "org.springframework.kafka.KafkaException$Level;"
                    + "org.springframework.core.NestedRuntimeException;"
                    + "org.apache.kafka.common.header.internals.RecordHeaders;"
                    + "org.apache.kafka.common.header.internals.RecordHeader;"
                    + "org.apache.kafka.common.header.Header;"
                    + "java.lang.*;"
                    + "java.util.Collections$*;"
                    + "java.util.ImmutableCollections$*;"
                    + "java.util.ArrayList;"
                    + "java.util.LinkedList;!*");

    public ExtractedMetadata extract(ConsumerRecord<byte[], byte[]> record) {
        Map<String, String> headersMap = new LinkedHashMap<>();
        String originalTopic = null;
        Integer originalPartition = null;
        Long originalOffset = null;
        String originalConsumerGroup = null;
        String exceptionFqcn = null;
        String exceptionCauseFqcn = null;
        String exceptionMessage = null;
        String exceptionStacktrace = null;
        int replayHops = 0;
        UUID correlatedPlanId = null;
        String originCoords = null;

        byte[] recoveredPayload = record.value();
        byte[] recoveredKey = record.key();
        boolean hasDeserException = false;

        for (Header header : record.headers()) {
            String key = header.key();
            byte[] val = header.value();
            if (val == null) {
                continue;
            }
            if (KafkaHeaders.DLT_ORIGINAL_TOPIC.equals(key)) {
                originalTopic = new String(val, StandardCharsets.UTF_8);
                headersMap.put(key, originalTopic);
            } else if (KafkaHeaders.DLT_ORIGINAL_PARTITION.equals(key)) {
                originalPartition = decodeIntHeader(val);
                headersMap.put(key, originalPartition != null ? originalPartition.toString() : "");
            } else if (KafkaHeaders.DLT_ORIGINAL_OFFSET.equals(key)) {
                originalOffset = decodeLongHeader(val);
                headersMap.put(key, originalOffset != null ? originalOffset.toString() : "");
            } else if (KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP.equals(key)) {
                originalConsumerGroup = new String(val, StandardCharsets.UTF_8);
                headersMap.put(key, originalConsumerGroup);
            } else if (KafkaHeaders.DLT_EXCEPTION_FQCN.equals(key)) {
                exceptionFqcn = new String(val, StandardCharsets.UTF_8);
                headersMap.put(key, exceptionFqcn);
            } else if (KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN.equals(key)) {
                exceptionCauseFqcn = new String(val, StandardCharsets.UTF_8);
                headersMap.put(key, exceptionCauseFqcn);
            } else if (KafkaHeaders.DLT_EXCEPTION_MESSAGE.equals(key)) {
                exceptionMessage = new String(val, StandardCharsets.UTF_8);
                headersMap.put(key, exceptionMessage);
            } else if (KafkaHeaders.DLT_EXCEPTION_STACKTRACE.equals(key)) {
                exceptionStacktrace = new String(val, StandardCharsets.UTF_8);
                headersMap.put(key, "[stacktrace:" + exceptionStacktrace.length() + " chars]");
            } else if (VALUE_DESER_EX_HEADER.equals(key)) {
                hasDeserException = true;
                DeserializationException deserEx = safeDeserializeException(val);
                if (deserEx != null) {
                    if (deserEx.getData() != null) {
                        recoveredPayload = deserEx.getData();
                    }
                    if (exceptionFqcn == null) {
                        exceptionFqcn = deserEx.getClass().getName();
                    }
                    if (exceptionMessage == null && deserEx.getMessage() != null) {
                        exceptionMessage = deserEx.getMessage();
                    }
                    if (exceptionCauseFqcn == null && deserEx.getCause() != null) {
                        exceptionCauseFqcn = deserEx.getCause().getClass().getName();
                    }
                }
                headersMap.put(key, "[DeserializationException:filtered]");
            } else if (KEY_DESER_EX_HEADER.equals(key)) {
                hasDeserException = true;
                DeserializationException deserEx = safeDeserializeException(val);
                if (deserEx != null && deserEx.getData() != null) {
                    recoveredKey = deserEx.getData();
                }
                headersMap.put(key, "[KeyDeserializationException:filtered]");
            } else {
                String strVal = new String(val, StandardCharsets.UTF_8);
                headersMap.put(key, strVal);
                String lower = key.toLowerCase(Locale.ROOT);
                if ("x-dlt-deck-replay-hops".equals(lower)) {
                    replayHops = parseIntOrZero(strVal);
                } else if ("x-dlt-deck-replay-plan-id".equals(lower)) {
                    correlatedPlanId = parseUuidOrNull(strVal);
                } else if ("x-dlt-deck-origin-coords".equals(lower)) {
                    originCoords = strVal;
                }
            }
        }

        return new ExtractedMetadata(
                originalTopic,
                originalPartition,
                originalOffset,
                originalConsumerGroup,
                exceptionFqcn,
                exceptionCauseFqcn,
                exceptionMessage,
                exceptionStacktrace,
                recoveredKey,
                recoveredPayload,
                hasDeserException,
                replayHops,
                correlatedPlanId,
                originCoords,
                headersMap);
    }

    /**
     * Deserializes a Spring Kafka {@link DeserializationException} byte array using a strict JEP 290/415
     * {@link ObjectInputFilter} so malicious serialized gadgets are rejected before instantiation.
     */
    public @Nullable DeserializationException safeDeserializeException(byte[] bytes) {
        if (bytes.length == 0 || bytes.length > 524_288) {
            return null;
        }
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            ois.setObjectInputFilter(STRICT_DESER_EXCEPTION_FILTER);
            Object obj = ois.readObject();
            if (obj instanceof DeserializationException de) {
                return de;
            }
            return null;
        } catch (Exception ex) {
            log.debug("Rejected or failed to safely deserialize DeserializationException header: {}", ex.getMessage());
            return null;
        }
    }

    static @Nullable Integer decodeIntHeader(byte[] val) {
        if (val.length == 4) {
            return ByteBuffer.wrap(val).getInt();
        }
        try {
            return Integer.parseInt(new String(val, StandardCharsets.UTF_8).trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    static @Nullable Long decodeLongHeader(byte[] val) {
        if (val.length == 8) {
            return ByteBuffer.wrap(val).getLong();
        }
        try {
            return Long.parseLong(new String(val, StandardCharsets.UTF_8).trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static int parseIntOrZero(String s) {
        try {
            return Math.max(0, Integer.parseInt(s.trim()));
        } catch (NumberFormatException ex) {
            return 0;
        }
    }

    private static @Nullable UUID parseUuidOrNull(String s) {
        try {
            return UUID.fromString(s.trim());
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    @SuppressWarnings("ArrayRecordComponent")
    public record ExtractedMetadata(
            @Nullable String originalTopic,
            @Nullable Integer originalPartition,
            @Nullable Long originalOffset,
            @Nullable String originalConsumerGroup,
            @Nullable String exceptionFqcn,
            @Nullable String exceptionCauseFqcn,
            @Nullable String exceptionMessage,
            @Nullable String exceptionStacktrace,
            byte @Nullable [] recoveredKey,
            byte @Nullable [] recoveredPayload,
            boolean hasDeserializationException,
            int replayHops,
            @Nullable UUID correlatedPlanId,
            @Nullable String originCoords,
            Map<String, String> headers) {

        public ExtractedMetadata(
                @Nullable String originalTopic,
                @Nullable Integer originalPartition,
                @Nullable Long originalOffset,
                @Nullable String originalConsumerGroup,
                @Nullable String exceptionFqcn,
                @Nullable String exceptionCauseFqcn,
                @Nullable String exceptionMessage,
                @Nullable String exceptionStacktrace,
                byte @Nullable [] recoveredKey,
                byte @Nullable [] recoveredPayload,
                boolean hasDeserializationException,
                int replayHops,
                @Nullable UUID correlatedPlanId,
                @Nullable String originCoords,
                Map<String, String> headers) {
            this.originalTopic = originalTopic;
            this.originalPartition = originalPartition;
            this.originalOffset = originalOffset;
            this.originalConsumerGroup = originalConsumerGroup;
            this.exceptionFqcn = exceptionFqcn;
            this.exceptionCauseFqcn = exceptionCauseFqcn;
            this.exceptionMessage = exceptionMessage;
            this.exceptionStacktrace = exceptionStacktrace;
            this.recoveredKey = recoveredKey == null ? null : Arrays.copyOf(recoveredKey, recoveredKey.length);
            this.recoveredPayload =
                    recoveredPayload == null ? null : Arrays.copyOf(recoveredPayload, recoveredPayload.length);
            this.hasDeserializationException = hasDeserializationException;
            this.replayHops = replayHops;
            this.correlatedPlanId = correlatedPlanId;
            this.originCoords = originCoords;
            this.headers = Map.copyOf(headers);
        }

        @Override
        public byte @Nullable [] recoveredKey() {
            return recoveredKey == null ? null : Arrays.copyOf(recoveredKey, recoveredKey.length);
        }

        @Override
        public byte @Nullable [] recoveredPayload() {
            return recoveredPayload == null ? null : Arrays.copyOf(recoveredPayload, recoveredPayload.length);
        }
    }
}
