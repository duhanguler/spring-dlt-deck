package io.github.duhanguler.dltdeck.starter.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.duhanguler.dltdeck.core.model.DltMessageDetail;
import io.github.duhanguler.dltdeck.core.model.FailureCategory;
import io.github.duhanguler.dltdeck.core.model.IngestedDltMessage;
import io.github.duhanguler.dltdeck.core.model.MessageStatus;
import io.github.duhanguler.dltdeck.core.model.PayloadFormat;
import io.github.duhanguler.dltdeck.core.model.ReplayOrderingMode;
import io.github.duhanguler.dltdeck.core.model.ReplayPlan;
import io.github.duhanguler.dltdeck.core.model.ReplayPlanStatus;
import io.github.duhanguler.dltdeck.core.patch.JsonPayloadPatcher;
import io.github.duhanguler.dltdeck.core.policy.AuditHashChain;
import io.github.duhanguler.dltdeck.core.policy.BlastRadiusGuard;
import io.github.duhanguler.dltdeck.core.policy.KeysetCursorCodec;
import io.github.duhanguler.dltdeck.core.policy.MakerCheckerPolicy;
import io.github.duhanguler.dltdeck.core.policy.ReplayHeaderSanitizer;
import io.github.duhanguler.dltdeck.starter.config.DltDeckProperties;
import io.github.duhanguler.dltdeck.starter.kafka.DltDeckIdempotentReplayFilter;
import io.github.duhanguler.dltdeck.starter.kafka.KeyStripedReplayExecutor;
import io.github.duhanguler.dltdeck.starter.kafka.TargetTopicValidator;
import io.github.duhanguler.dltdeck.starter.service.LocalRateLimitCoordinator;
import io.github.duhanguler.dltdeck.storage.jdbc.codec.PayloadStorageCodec;
import io.github.duhanguler.dltdeck.storage.jdbc.migration.DltDeckDatabaseDialect;
import io.github.duhanguler.dltdeck.storage.jdbc.migration.DltDeckSchemaMigrator;
import io.github.duhanguler.dltdeck.storage.jdbc.repository.JdbcAuditLedgerRepository;
import io.github.duhanguler.dltdeck.storage.jdbc.repository.JdbcDltMessageRepository;
import io.github.duhanguler.dltdeck.storage.jdbc.repository.JdbcReplayPlanRepository;
import io.github.duhanguler.dltdeck.storage.jdbc.repository.JdbcStripeLeaseStore;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

/**
 * Fault-injection test proving honest crash semantics:
 * when a pod crashes AFTER Kafka broker ACK and BEFORE TX2 DB commit, advancing {@link Clock} past lease
 * expiry transitions the record strictly to {@code INDETERMINATE} (never silently double-replayed!),
 * and {@link DltDeckIdempotentReplayFilter} guarantees consumer-side effective exactly-once on manual recovery.
 */
class CrashRecoveryIndeterminateIT {

    @Test
    void crashBetweenKafkaAckAndTx2TransitionsToIndeterminateNeverSilentDoubleReplay() throws Exception {
        String dltTopic = "crash-orders.dlt";
        String targetTopic = "crash-orders-" + UUID.randomUUID().toString().substring(0, 8);
        SharedContainers.createTopics(Map.of(dltTopic, 1, targetTopic, 1));

        Instant[] current = {Instant.parse("2026-11-24T12:00:00Z")};
        Clock mutableClock = new Clock() {
            @Override
            public ZoneId getZone() {
                return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                return current[0];
            }
        };

        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(SharedContainers.POSTGRES.getJdbcUrl());
        hc.setUsername(SharedContainers.POSTGRES.getUsername());
        hc.setPassword(SharedContainers.POSTGRES.getPassword());

        try (HikariDataSource ds = new HikariDataSource(hc);
                AdminClient admin = AdminClient.create(
                        Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, SharedContainers.KAFKA.getBootstrapServers()))) {

            new DltDeckSchemaMigrator(ds, mutableClock).migrate();
            SharedContainers.truncateDltDeckTables(new JdbcTemplate(ds));

            JsonMapper mapper = JsonMapper.builder().build();
            var msgRepo = new JdbcDltMessageRepository(
                    ds,
                    DltDeckDatabaseDialect.POSTGRESQL,
                    new PayloadStorageCodec(1_048_576, 4096, true, null),
                    new KeysetCursorCodec("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8)),
                    mapper);
            var planRepo = new JdbcReplayPlanRepository(ds);
            var stripeStore = new JdbcStripeLeaseStore(ds, DltDeckDatabaseDialect.POSTGRESQL);
            var auditRepo = new JdbcAuditLedgerRepository(ds, DltDeckDatabaseDialect.POSTGRESQL, new AuditHashChain());

            UUID msgId = UUID.randomUUID();
            byte[] payload = "{\"orderId\":\"ORD-CRASH-1\"}".getBytes(StandardCharsets.UTF_8);
            msgRepo.ingestBatch(List.of(new IngestedDltMessage(
                    msgId, dltTopic, 0, 1L, targetTopic, 0, 1L, "cg", "k-crash".getBytes(StandardCharsets.UTF_8),
                    1, "kfp-crash", payload, PayloadFormat.JSON, null, payload.length,
                    "fp-crash", "Ex", "Ex", "timeout", "stack", FailureCategory.TRANSIENT_DOWNSTREAM, "retry",
                    0, MessageStatus.NEW, null, Map.of(), current[0])));

            UUID planId = UUID.randomUUID();
            String specHash = new MakerCheckerPolicy()
                    .computePlanSpecHash("Crash Test", "fp-crash", dltTopic, targetTopic, ReplayOrderingMode.PRESERVE_KEY_ORDER, null, 100, 10, 1);
            planRepo.save(new ReplayPlan(
                    planId, "Crash Test", "fp-crash", dltTopic, targetTopic, ReplayOrderingMode.PRESERVE_KEY_ORDER,
                    null, 100, 10, 1, specHash, ReplayPlanStatus.APPROVED, true, "op", "approver", null,
                    0, 0, 0, 0, 0, 0, current[0], current[0], null));

            DltDeckProperties props = new DltDeckProperties();
            props.getReplay().setStripeCount(1);
            props.getReplay().setMessageLeaseDuration(Duration.ofSeconds(30));

            KafkaTemplate<byte[], byte[]> kt = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SharedContainers.KAFKA.getBootstrapServers(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                    ProducerConfig.ACKS_CONFIG, "all",
                    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)));

            KeyStripedReplayExecutor executor = new KeyStripedReplayExecutor(
                    kt, msgRepo, planRepo, stripeStore, auditRepo, new LocalRateLimitCoordinator(mutableClock),
                    new TargetTopicValidator(admin, true), new JsonPayloadPatcher(mapper, 1_048_576, 32),
                    new ReplayHeaderSanitizer(mutableClock), new MakerCheckerPolicy(),
                    new BlastRadiusGuard(mutableClock, 10, 0.5, Duration.ofMinutes(5)),
                    new AuditHashChain(), props, mutableClock);

            // Inject simulated pod crash strictly AFTER Kafka broker ACK and BEFORE TX2 DB commit!
            executor.setFaultHook(new KeyStripedReplayExecutor.FaultInjectionHook() {
                @Override
                public boolean simulateCrashAfterBrokerAckBeforeTx2(DltMessageDetail detail, long fencingToken) {
                    return true;
                }
            });

            executor.executePlan(planId, "pod-crasher");

            // Record is STILL CLAIMED in DB even though Kafka broker already committed it
            assertThat(msgRepo.findDetailById(msgId).orElseThrow().summary().status()).isEqualTo(MessageStatus.CLAIMED);

            // Advance Clock past 30s lease expiry and sweep -> MUST become INDETERMINATE (NEVER NEW!)
            current[0] = current[0].plusSeconds(45);
            var swept = executor.sweepExpiredLeases();
            assertThat(swept).hasSize(1);
            assertThat(msgRepo.findDetailById(msgId).orElseThrow().summary().status())
                    .isEqualTo(MessageStatus.INDETERMINATE);

            // Verify record is NOT eligible for automatic silent replay
            assertThat(msgRepo.findEligibleForStripe(dltTopic, "fp-crash", 1, 0, 10)).isEmpty();

            // Verify Kafka broker DID receive the record with deterministic x-dlt-deck-idempotency-key
            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SharedContainers.KAFKA.getBootstrapServers(),
                    ConsumerConfig.GROUP_ID_CONFIG, "verify-crash-" + UUID.randomUUID(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName()))) {
                consumer.subscribe(List.of(targetTopic));
                ConsumerRecord<byte[], byte[]> delivered = null;
                long deadline = System.currentTimeMillis() + 8_000;
                while (System.currentTimeMillis() < deadline && delivered == null) {
                    for (var rec : consumer.poll(Duration.ofMillis(200))) {
                        delivered = rec;
                    }
                }
                assertThat(delivered).isNotNull();
                DltDeckIdempotentReplayFilter filter = new DltDeckIdempotentReplayFilter();
                assertThat(filter.extractIdempotencyKey(delivered)).hasSize(64);
                assertThat(filter.shouldProcess(delivered)).isTrue();
                assertThat(filter.shouldProcess(delivered)).isFalse(); // consumer-side deduplication prevents duplicate!
            }
        }
    }
}
