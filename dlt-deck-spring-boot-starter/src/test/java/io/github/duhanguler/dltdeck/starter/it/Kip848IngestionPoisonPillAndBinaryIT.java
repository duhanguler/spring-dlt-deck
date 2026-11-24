package io.github.duhanguler.dltdeck.starter.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.duhanguler.dltdeck.core.codec.DefaultPayloadCodec;
import io.github.duhanguler.dltdeck.core.context.ActorContext;
import io.github.duhanguler.dltdeck.core.context.ActorRole;
import io.github.duhanguler.dltdeck.core.exception.ValidationDomainException;
import io.github.duhanguler.dltdeck.core.fingerprint.ExceptionFingerprinter;
import io.github.duhanguler.dltdeck.core.masking.PiiRedactor;
import io.github.duhanguler.dltdeck.core.model.MessageStatus;
import io.github.duhanguler.dltdeck.core.model.PayloadFormat;
import io.github.duhanguler.dltdeck.core.patch.JsonPayloadPatcher;
import io.github.duhanguler.dltdeck.core.policy.AuditHashChain;
import io.github.duhanguler.dltdeck.core.policy.BlastRadiusGuard;
import io.github.duhanguler.dltdeck.core.policy.KeysetCursorCodec;
import io.github.duhanguler.dltdeck.core.policy.MakerCheckerPolicy;
import io.github.duhanguler.dltdeck.core.policy.ReplayHeaderSanitizer;
import io.github.duhanguler.dltdeck.starter.config.DltDeckProperties;
import io.github.duhanguler.dltdeck.starter.kafka.DltBatchIngestionEngine;
import io.github.duhanguler.dltdeck.starter.kafka.KeyStripedReplayExecutor;
import io.github.duhanguler.dltdeck.starter.kafka.SafeSpringHeaderExtractor;
import io.github.duhanguler.dltdeck.starter.kafka.TargetTopicValidator;
import io.github.duhanguler.dltdeck.starter.kafka.TieredDryRunService;
import io.github.duhanguler.dltdeck.starter.service.DltDeckControlPlaneService;
import io.github.duhanguler.dltdeck.starter.service.LocalRateLimitCoordinator;
import io.github.duhanguler.dltdeck.storage.jdbc.codec.PayloadStorageCodec;
import io.github.duhanguler.dltdeck.storage.jdbc.migration.DltDeckDatabaseDialect;
import io.github.duhanguler.dltdeck.storage.jdbc.migration.DltDeckSchemaMigrator;
import io.github.duhanguler.dltdeck.storage.jdbc.repository.JdbcAuditLedgerRepository;
import io.github.duhanguler.dltdeck.storage.jdbc.repository.JdbcDltMessageRepository;
import io.github.duhanguler.dltdeck.storage.jdbc.repository.JdbcReplayPlanRepository;
import io.github.duhanguler.dltdeck.storage.jdbc.repository.JdbcStripeLeaseStore;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Verifies KIP-848 batch ingestion against real Apache Kafka 4 KRaft + PostgreSQL 17:
 * poison-pill {@code ErrorHandlingDeserializer} raw byte recovery via {@code ObjectInputFilter},
 * Confluent {@code 0x00} binary {@code NOT_PATCHABLE} guard, tombstones, deduplicated fingerprints,
 * post-DB manual offset commit, and accurate live {@code ingestionLag}.
 */
class Kip848IngestionPoisonPillAndBinaryIT {

    @Test
    void ingestsPoisonPillConfluentBinaryTombstoneAndDeduplicatesClusters() throws Exception {
        String dltTopic = "orders-kip.dlt";
        SharedContainers.createTopics(Map.of(dltTopic, 1, "orders-kip", 1));

        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(SharedContainers.POSTGRES.getJdbcUrl());
        hc.setUsername(SharedContainers.POSTGRES.getUsername());
        hc.setPassword(SharedContainers.POSTGRES.getPassword());

        try (HikariDataSource ds = new HikariDataSource(hc);
                AdminClient admin = AdminClient.create(
                        Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, SharedContainers.KAFKA.getBootstrapServers()));
                KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SharedContainers.KAFKA.getBootstrapServers(),
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()))) {

            Clock clock = Clock.systemUTC();
            new DltDeckSchemaMigrator(ds, clock).migrate();
            SharedContainers.truncateDltDeckTables(new JdbcTemplate(ds));

            JsonMapper mapper = JsonMapper.builder().build();
            DltDeckProperties props = new DltDeckProperties();
            props.getIngestion().setAllowTopics(List.of(dltTopic));
            props.getIngestion().setDiscoveryInterval(Duration.ofSeconds(1));
            props.getIngestion().setPollTimeout(Duration.ofMillis(200));
            props.getIngestion().setGroupId("dlt-deck-kip848-it-" + System.nanoTime());

            var msgRepo = new JdbcDltMessageRepository(
                    ds,
                    DltDeckDatabaseDialect.POSTGRESQL,
                    new PayloadStorageCodec(1_048_576, 4096, true, null),
                    new KeysetCursorCodec("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8)),
                    mapper);
            var planRepo = new JdbcReplayPlanRepository(ds);
            var stripeStore = new JdbcStripeLeaseStore(ds, DltDeckDatabaseDialect.POSTGRESQL);
            var auditRepo = new JdbcAuditLedgerRepository(ds, DltDeckDatabaseDialect.POSTGRESQL, new AuditHashChain());
            var blastGuard = new BlastRadiusGuard(clock, 10, 0.3, Duration.ofMinutes(5));
            var codec = new DefaultPayloadCodec(mapper);
            var patcher = new JsonPayloadPatcher(mapper, 1_048_576, 32);
            var kafkaTemplate = new KafkaTemplate<byte[], byte[]>(new DefaultKafkaProducerFactory<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SharedContainers.KAFKA.getBootstrapServers(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class)));

            var engine = new DltBatchIngestionEngine(
                    Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, SharedContainers.KAFKA.getBootstrapServers()),
                    admin,
                    props,
                    new SafeSpringHeaderExtractor(),
                    codec,
                    new ExceptionFingerprinter(),
                    msgRepo,
                    planRepo,
                    stripeStore,
                    blastGuard,
                    clock);

            var service = new DltDeckControlPlaneService(
                    msgRepo,
                    planRepo,
                    auditRepo,
                    engine,
                    new KeyStripedReplayExecutor(
                            kafkaTemplate, msgRepo, planRepo, stripeStore, auditRepo,
                            new LocalRateLimitCoordinator(clock), new TargetTopicValidator(admin, true),
                            patcher, new ReplayHeaderSanitizer(clock), new MakerCheckerPolicy(),
                            blastGuard, new AuditHashChain(), props, clock),
                    new TieredDryRunService(
                            msgRepo, patcher, codec, mapper, null, List.of(),
                            new TargetTopicValidator(admin, true), new ReplayHeaderSanitizer(clock), kafkaTemplate),
                    new TargetTopicValidator(admin, true),
                    codec,
                    new PiiRedactor(),
                    patcher,
                    new MakerCheckerPolicy(),
                    props,
                    mapper,
                    clock);

            // 1. Poison-pill ErrorHandlingDeserializer record (raw bytes inside DeserializationException header)
            byte[] rawPoison = "{corrupt-order-json".getBytes(StandardCharsets.UTF_8);
            DeserializationException deserEx = new DeserializationException("JSON parse failed", rawPoison, false, null);
            ProducerRecord<byte[], byte[]> poisonRec = new ProducerRecord<>(dltTopic, "k-poison".getBytes(StandardCharsets.UTF_8), null);
            poisonRec.headers().add(new RecordHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC, "orders-kip".getBytes(StandardCharsets.UTF_8)));
            poisonRec.headers().add(new RecordHeader(SafeSpringHeaderExtractor.VALUE_DESER_EX_HEADER, serialize(deserEx)));
            producer.send(poisonRec).get();

            // 2. Confluent Schema Registry 0x00 wire format binary record (schemaId=42)
            byte[] confluentWire = ByteBuffer.allocate(16).put((byte) 0x00).putInt(42).putLong(0xCAFEBABEL).array();
            ProducerRecord<byte[], byte[]> confluentRec = new ProducerRecord<>(dltTopic, "k-avro".getBytes(StandardCharsets.UTF_8), confluentWire);
            confluentRec.headers().add(new RecordHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC, "orders-kip".getBytes(StandardCharsets.UTF_8)));
            confluentRec.headers().add(new RecordHeader(KafkaHeaders.DLT_EXCEPTION_FQCN, "io.confluent.kafka.serializers.SchemaRegistryException".getBytes(StandardCharsets.UTF_8)));
            producer.send(confluentRec).get();

            // 3. Kafka Tombstone (value == null, no deser header)
            ProducerRecord<byte[], byte[]> tombstoneRec = new ProducerRecord<>(dltTopic, "k-tomb".getBytes(StandardCharsets.UTF_8), null);
            tombstoneRec.headers().add(new RecordHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC, "orders-kip".getBytes(StandardCharsets.UTF_8)));
            tombstoneRec.headers().add(new RecordHeader(KafkaHeaders.DLT_EXCEPTION_FQCN, "java.lang.IllegalStateException".getBytes(StandardCharsets.UTF_8)));
            producer.send(tombstoneRec).get();

            // 4 & 5. Two duplicate validation failures with different UUIDs -> must collapse to 1 fingerprint cluster
            for (String uid : List.of("11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222")) {
                ProducerRecord<byte[], byte[]> dup = new ProducerRecord<>(
                        dltTopic,
                        ("k-" + uid).getBytes(StandardCharsets.UTF_8),
                        ("{\"orderId\":\"" + uid + "\",\"currency\":\"TRL\"}").getBytes(StandardCharsets.UTF_8));
                dup.headers().add(new RecordHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC, "orders-kip".getBytes(StandardCharsets.UTF_8)));
                dup.headers().add(new RecordHeader(KafkaHeaders.DLT_EXCEPTION_FQCN, "jakarta.validation.ConstraintViolationException".getBytes(StandardCharsets.UTF_8)));
                dup.headers().add(new RecordHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE, ("Invalid currency for " + uid).getBytes(StandardCharsets.UTF_8)));
                producer.send(dup).get();
            }
            producer.flush();

            engine.start();
            try {
                long deadline = System.currentTimeMillis() + 20_000;
                while (System.currentTimeMillis() < deadline) {
                    if (msgRepo.listMessages(dltTopic, null, null, null, 20).items().size() >= 5) {
                        break;
                    }
                    Thread.sleep(200);
                }
            } finally {
                engine.stop();
            }

            var messages = msgRepo.listMessages(dltTopic, null, null, null, 20).items();
            assertThat(messages).hasSize(5);
            assertThat(engine.totalIngestionLag()).isZero();

            // Verify poison pill recovered raw bytes
            var poisonMsg = messages.stream()
                    .filter(m -> m.payloadFormat() == PayloadFormat.UTF8_TEXT)
                    .findFirst()
                    .orElseThrow();
            var poisonDetail = msgRepo.findDetailById(poisonMsg.id()).orElseThrow();
            assertThat(new String(poisonDetail.payload(), StandardCharsets.UTF_8)).isEqualTo("{corrupt-order-json");

            // Verify Confluent binary guarded as NOT_PATCHABLE
            var confluentMsg = messages.stream()
                    .filter(m -> m.payloadFormat() == PayloadFormat.CONFLUENT_SCHEMA_REGISTRY)
                    .findFirst()
                    .orElseThrow();
            assertThat(confluentMsg.schemaId()).isEqualTo(42);
            ActorContext.runAs(new ActorContext("op", ActorRole.OPERATOR, null), () ->
                    assertThatThrownBy(() -> service.previewPatch(confluentMsg.id(), "{\"a\":1}"))
                            .isInstanceOf(ValidationDomainException.class)
                            .extracting("errorCode")
                            .isEqualTo("NOT_PATCHABLE"));

            // Verify tombstone recognized
            assertThat(messages.stream().filter(m -> m.payloadFormat() == PayloadFormat.TOMBSTONE).count()).isEqualTo(1);

            // Verify 2 duplicate validation errors collapsed to 1 fingerprint cluster
            var clusters = msgRepo.listClusters(dltTopic, 20);
            assertThat(clusters).hasSize(4); // 5 messages -> 4 distinct fingerprint clusters
            assertThat(clusters.stream().filter(c -> c.totalCount() == 2).count()).isEqualTo(1);
            assertThat(messages).allMatch(m -> m.status() == MessageStatus.NEW);
        }
    }

    private static byte[] serialize(Object obj) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
        }
        return baos.toByteArray();
    }
}
