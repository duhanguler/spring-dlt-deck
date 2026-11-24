package io.github.duhanguler.dltdeck.starter;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.duhanguler.dltdeck.core.policy.ReplayHeaderSanitizer;
import io.github.duhanguler.dltdeck.starter.kafka.DltDeckIdempotentReplayFilter;
import io.github.duhanguler.dltdeck.starter.kafka.SafeSpringHeaderExtractor;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.SerializationUtils;

class SafeSpringHeaderExtractorAndIdempotentFilterTest {

    static final class ForbiddenGadget implements Serializable {
        private static final long serialVersionUID = 1L;
        final String payload = "evil";
    }

    @Test
    void decodesBinarySpringDltHeadersRecoversPoisonPillBytesAndBlocksGadgets() throws Exception {
        SafeSpringHeaderExtractor extractor = new SafeSpringHeaderExtractor();

        byte[] rawPoison = "{corrupt-json".getBytes(StandardCharsets.UTF_8);
        DeserializationException deserEx = new DeserializationException("Cannot parse JSON", rawPoison, false, null);
        byte[] serializedDeserEx = serialize(deserEx);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "orders.dlt", 0, 55L, "k1".getBytes(StandardCharsets.UTF_8), null);
        record.headers().add(new RecordHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC, "orders".getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer.allocate(4).putInt(3).array()));
        record.headers().add(new RecordHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(8).putLong(999L).array()));
        record.headers().add(new RecordHeader(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, serializedDeserEx));
        record.headers().add(new RecordHeader("x-dlt-deck-replay-hops", "2".getBytes(StandardCharsets.UTF_8)));

        var extracted = extractor.extract(record);
        assertThat(extracted.originalTopic()).isEqualTo("orders");
        assertThat(extracted.originalPartition()).isEqualTo(3);
        assertThat(extracted.originalOffset()).isEqualTo(999L);
        assertThat(extracted.hasDeserializationException()).isTrue();
        assertThat(extracted.recoveredPayload()).isEqualTo(rawPoison);
        assertThat(extracted.replayHops()).isEqualTo(2);

        // ObjectInputFilter MUST reject non-allowlisted classes
        byte[] gadgetBytes = serialize(new ForbiddenGadget());
        assertThat(extractor.safeDeserializeException(gadgetBytes)).isNull();
    }

    @Test
    void idempotentReplayFilterDeduplicatesByIdempotencyKeyHeader() {
        DltDeckIdempotentReplayFilter filter = new DltDeckIdempotentReplayFilter();

        ConsumerRecord<String, String> normal = new ConsumerRecord<>("orders", 0, 1L, "k", "{}");
        assertThat(filter.shouldProcess(normal)).isTrue();
        assertThat(filter.shouldProcess(normal)).isTrue(); // non-replayed always allowed

        ConsumerRecord<String, String> replayed = new ConsumerRecord<>("orders", 0, 2L, "k", "{}");
        replayed.headers().add(new RecordHeader(
                ReplayHeaderSanitizer.HEADER_IDEMPOTENCY_KEY,
                "sha256-key-1".getBytes(StandardCharsets.UTF_8)));
        assertThat(filter.shouldProcess(replayed)).isTrue();
        assertThat(filter.shouldProcess(replayed)).isFalse(); // duplicate blocked!
    }

    private static byte[] serialize(Object obj) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
        }
        return baos.toByteArray();
    }
}
