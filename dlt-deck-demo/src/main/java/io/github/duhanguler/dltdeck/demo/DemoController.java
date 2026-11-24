package io.github.duhanguler.dltdeck.demo;

import io.github.duhanguler.dltdeck.starter.kafka.SafeSpringHeaderExtractor;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Showcases Spring Framework 7 Native API Versioning ({@code version = "1.0"}) in the host app layer
 * alongside the 5-scenario Chaos Incident Simulator for instant live Workbench evaluation.
 */
@RestController
@RequestMapping("/api")
public class DemoController {

    private static final String PAYMENTS_TOPIC = "payments.events";
    private static final String PAYMENTS_DLT = "payments.events.dlt";

    private final PaymentOrderRepository orderRepository;
    private final KafkaTemplate<byte[], byte[]> kafkaTemplate;

    public DemoController(
            PaymentOrderRepository orderRepository,
            @Qualifier("dltDeckInternal") KafkaTemplate<byte[], byte[]> kafkaTemplate) {
        this.orderRepository = orderRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping(path = "/orders", version = "1.0")
    public List<PaymentOrderEntity> listOrdersV1() {
        return orderRepository.findAll();
    }

    @PostMapping("/chaos/trigger")
    public Map<String, Object> triggerAllFiveChaosScenarios() throws Exception {
        int published = 0;

        // Scenario 1: Same-key ordered chain (CREATED -> AUTHORIZED -> CAPTURED)
        String chainKey = "ORD-CHAIN-1001";
        List<String> stages = List.of("CREATED", "AUTHORIZED", "CAPTURED");
        for (int i = 0; i < stages.size(); i++) {
            String json = """
                    {"orderId":"%s","customerName":"Ada Lovelace","currency":"TRY","amountCents":24900,"stage":"%s","seq":%d}
                    """.formatted(chainKey, stages.get(i), i + 1);
            ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(
                    PAYMENTS_DLT, chainKey.getBytes(StandardCharsets.UTF_8), json.getBytes(StandardCharsets.UTF_8));
            addDltHeaders(rec, PAYMENTS_TOPIC, 0, 100L + i, "java.net.SocketTimeoutException",
                    "Downstream payment gateway 503 timeout for order " + UUID.randomUUID());
            kafkaTemplate.send(rec).get();
            published++;
        }

        // Scenario 2: Poison-pill ErrorHandlingDeserializer corrupt JSON with real checksum-valid PII
        byte[] corruptPayload = """
                {"orderId":"ORD-POISON-1","tckn":"10000000146","iban":"TR330006100519786457841326","card":"4532-0151-1283-0366","email":"vip.customer@acme.com","amount":BROKEN
                """.getBytes(StandardCharsets.UTF_8);
        DeserializationException deserEx =
                new DeserializationException("Unexpected character 'B' parsing payment JSON", corruptPayload, false, null);
        ProducerRecord<byte[], byte[]> poisonRec =
                new ProducerRecord<>(PAYMENTS_DLT, "ORD-POISON-1".getBytes(StandardCharsets.UTF_8), null);
        addDltHeaders(poisonRec, PAYMENTS_TOPIC, 1, 200L, DeserializationException.class.getName(),
                "Cannot deserialize corrupt JSON poison pill");
        poisonRec.headers().add(new RecordHeader(SafeSpringHeaderExtractor.VALUE_DESER_EX_HEADER, serialize(deserEx)));
        kafkaTemplate.send(poisonRec).get();
        published++;

        // Scenario 3: Confluent Schema Registry 0x00 binary payload (schemaId = 42, NOT_PATCHABLE)
        byte[] confluentWire = ByteBuffer.allocate(24).put((byte) 0x00).putInt(42).putLong(0x1122334455667788L).array();
        ProducerRecord<byte[], byte[]> confluentRec =
                new ProducerRecord<>(PAYMENTS_DLT, "ORD-AVRO-42".getBytes(StandardCharsets.UTF_8), confluentWire);
        addDltHeaders(confluentRec, PAYMENTS_TOPIC, 2, 300L, "io.confluent.kafka.serializers.SchemaRegistryException",
                "Incompatible Avro reader schema for schemaId=42");
        kafkaTemplate.send(confluentRec).get();
        published++;

        // Scenario 4: 5 Validation Failures (deprecated currency "TRL" -> patchable via RFC 7396 {"currency":"TRY"})
        for (int i = 1; i <= 5; i++) {
            String oid = "ORD-VAL-" + (500 + i);
            String json = """
                    {"orderId":"%s","customerName":"Customer %d","currency":"TRL","amountCents":%d,"stage":"CREATED","email":"user%d@shop.tr","tckn":"10000000146"}
                    """.formatted(oid, i, 15000 + i * 500, i);
            ProducerRecord<byte[], byte[]> valRec =
                    new ProducerRecord<>(PAYMENTS_DLT, oid.getBytes(StandardCharsets.UTF_8), json.getBytes(StandardCharsets.UTF_8));
            addDltHeaders(valRec, PAYMENTS_TOPIC, i % 3, 400L + i, "jakarta.validation.ConstraintViolationException",
                    "Order " + UUID.randomUUID() + " failed validation: unsupported currency TRL");
            valRec.headers().add(new RecordHeader("ce_type", "com.acme.payments.created".getBytes(StandardCharsets.UTF_8)));
            valRec.headers().add(new RecordHeader("ce_id", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));
            valRec.headers().add(new RecordHeader("ce_source", "/checkout-service".getBytes(StandardCharsets.UTF_8)));
            kafkaTemplate.send(valRec).get();
            published++;
        }

        // Scenario 5: Replay Loop Quarantine record (replayHops = 3 >= maxReplayHops)
        ProducerRecord<byte[], byte[]> loopRec = new ProducerRecord<>(
                PAYMENTS_DLT,
                "ORD-LOOP-999".getBytes(StandardCharsets.UTF_8),
                "{\"orderId\":\"ORD-LOOP-999\",\"currency\":\"TRY\",\"amountCents\":-500}".getBytes(StandardCharsets.UTF_8));
        addDltHeaders(loopRec, PAYMENTS_TOPIC, 0, 999L, "java.lang.IllegalStateException",
                "Negative amount loop failure");
        loopRec.headers().add(new RecordHeader("x-dlt-deck-replay-hops", "3".getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(loopRec).get();
        published++;

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("status", "TRIGGERED");
        summary.put("publishedToDlt", published);
        summary.put("dltTopic", PAYMENTS_DLT);
        summary.put("scenarios", List.of(
                "1_KEY_ORDERED_CHAIN_3_EVENTS",
                "2_POISON_PILL_ERROR_HANDLING_DESERIALIZER_WITH_PII",
                "3_CONFLUENT_0x00_BINARY_SCHEMA_ID_42_NOT_PATCHABLE",
                "4_VALIDATION_CLUSTER_5_RECORDS_PATCHABLE_JSON",
                "5_REPLAY_LOOP_QUARANTINE_HOP_3"));
        return summary;
    }

    private static void addDltHeaders(
            ProducerRecord<byte[], byte[]> rec,
            String originalTopic,
            int originalPartition,
            long originalOffset,
            String exceptionFqcn,
            String exceptionMessage) {
        rec.headers().add(new RecordHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC, originalTopic.getBytes(StandardCharsets.UTF_8)));
        rec.headers().add(new RecordHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer.allocate(4).putInt(originalPartition).array()));
        rec.headers().add(new RecordHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(8).putLong(originalOffset).array()));
        rec.headers().add(new RecordHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP, "payments-demo-consumer".getBytes(StandardCharsets.UTF_8)));
        rec.headers().add(new RecordHeader(KafkaHeaders.DLT_EXCEPTION_FQCN, exceptionFqcn.getBytes(StandardCharsets.UTF_8)));
        rec.headers().add(new RecordHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE, exceptionMessage.getBytes(StandardCharsets.UTF_8)));
        rec.headers().add(new RecordHeader(
                KafkaHeaders.DLT_EXCEPTION_STACKTRACE,
                (exceptionFqcn + ": " + exceptionMessage + "\n\tat io.github.duhanguler.dltdeck.demo.PaymentConsumerService.onPaymentEvent(PaymentConsumerService.java:42)")
                        .getBytes(StandardCharsets.UTF_8)));
    }

    private static byte[] serialize(Object obj) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
        }
        return baos.toByteArray();
    }
}
