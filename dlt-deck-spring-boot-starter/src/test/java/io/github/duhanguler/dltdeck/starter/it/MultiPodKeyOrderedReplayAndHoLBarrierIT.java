package io.github.duhanguler.dltdeck.starter.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.duhanguler.dltdeck.core.fingerprint.ExceptionFingerprinter;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Multi-pod fault-injection test: runs TWO concurrent {@link KeyStripedReplayExecutor} pods against
 * PostgreSQL 17 + Kafka 4 KRaft on the same {@code PRESERVE_KEY_ORDER} plan across 5 keys x 6 events,
 * injecting a failure on {@code k2} event {@code seq=3}.
 *
 * <p>Proves strict per-key FIFO order {@code [1..6]} with zero duplicates for {@code k1, k3, k4, k5},
 * while {@code k2} stops after {@code [1, 2]} and marks {@code [4, 5, 6]} as {@code BLOCKED_BY_PRIOR_KEY_FAILURE}.
 */
class MultiPodKeyOrderedReplayAndHoLBarrierIT {

    @Test
    void twoConcurrentPodsPreservePerKeyOrderAndEnforceHolBarrierOnFailure() throws Exception {
        String dltTopic = "hol-orders.dlt";
        String targetTopic = "hol-orders-" + UUID.randomUUID().toString().substring(0, 8);
        SharedContainers.createTopics(Map.of(dltTopic, 4, targetTopic, 4));

        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(SharedContainers.POSTGRES.getJdbcUrl());
        hc.setUsername(SharedContainers.POSTGRES.getUsername());
        hc.setPassword(SharedContainers.POSTGRES.getPassword());
        hc.setMaximumPoolSize(12);

        try (HikariDataSource ds = new HikariDataSource(hc);
                AdminClient admin = AdminClient.create(
                        Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, SharedContainers.KAFKA.getBootstrapServers()))) {

            Clock clock = Clock.systemUTC();
            new DltDeckSchemaMigrator(ds, clock).migrate();
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

            Instant now = clock.instant();
            List<IngestedDltMessage> seed = new ArrayList<>();
            long offsetCounter = 0;
            for (int keyIdx = 1; keyIdx <= 5; keyIdx++) {
                String key = "order-k" + keyIdx;
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                String keyFp = ExceptionFingerprinter.sha256Hex(key);
                int origPart = keyIdx % 4;
                for (int seq = 1; seq <= 6; seq++) {
                    byte[] payload = ("{\"key\":\"" + key + "\",\"seq\":" + seq + "}").getBytes(StandardCharsets.UTF_8);
                    seed.add(new IngestedDltMessage(
                            UUID.randomUUID(),
                            dltTopic,
                            origPart,
                            ++offsetCounter,
                            targetTopic,
                            origPart,
                            (long) seq,
                            "orders-cg",
                            keyBytes,
                            Arrays.hashCode(keyBytes),
                            keyFp,
                            payload,
                            PayloadFormat.JSON,
                            null,
                            payload.length,
                            "fp-hol-test",
                            "TransientException",
                            "TransientException",
                            "downstream 503",
                            "stack",
                            FailureCategory.TRANSIENT_DOWNSTREAM,
                            "retry",
                            0,
                            MessageStatus.NEW,
                            null,
                            Map.of(),
                            now.plusMillis(offsetCounter)));
                }
            }
            assertThat(msgRepo.ingestBatch(seed)).isEqualTo(30);

            UUID planId = UUID.randomUUID();
            MakerCheckerPolicy mc = new MakerCheckerPolicy();
            String specHash = mc.computePlanSpecHash(
                    "Multi-Pod HoL Plan", "fp-hol-test", dltTopic, targetTopic, ReplayOrderingMode.PRESERVE_KEY_ORDER, null, 500, 100, 30);
            planRepo.save(new ReplayPlan(
                    planId, "Multi-Pod HoL Plan", "fp-hol-test", dltTopic, targetTopic,
                    ReplayOrderingMode.PRESERVE_KEY_ORDER, null, 500, 100, 30, specHash,
                    ReplayPlanStatus.APPROVED, true, "op", "approver", null,
                    0, 0, 0, 0, 0, 0, now, now, null));

            KeyStripedReplayExecutor.FaultInjectionHook failK2Seq3 = new KeyStripedReplayExecutor.FaultInjectionHook() {
                @Override
                public void beforeKafkaSend(DltMessageDetail detail, long fencingToken) {
                    String payload = new String(detail.payload(), StandardCharsets.UTF_8);
                    if (payload.contains("\"key\":\"order-k2\"") && payload.contains("\"seq\":3")) {
                        throw new RuntimeException("Simulated broker failure on k2 seq=3");
                    }
                }
            };

            KeyStripedReplayExecutor pod1 = createExecutor("pod-alpha", admin, msgRepo, planRepo, stripeStore, auditRepo, mapper, clock);
            KeyStripedReplayExecutor pod2 = createExecutor("pod-beta", admin, msgRepo, planRepo, stripeStore, auditRepo, mapper, clock);
            pod1.setFaultHook(failK2Seq3);
            pod2.setFaultHook(failK2Seq3);

            try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
                Future<ReplayPlan> f1 = exec.submit(() -> pod1.executePlan(planId, "pod-alpha"));
                Future<ReplayPlan> f2 = exec.submit(() -> pod2.executePlan(planId, "pod-beta"));
                f1.get();
                f2.get();
            }

            // Read all delivered records from Kafka targetTopic
            Map<String, List<Integer>> deliveredByKey = new HashMap<>();
            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SharedContainers.KAFKA.getBootstrapServers(),
                    ConsumerConfig.GROUP_ID_CONFIG, "verify-hol-" + UUID.randomUUID(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName()))) {
                consumer.subscribe(List.of(targetTopic));
                long deadline = System.currentTimeMillis() + 10_000;
                int total = 0;
                while (System.currentTimeMillis() < deadline && total < 26) {
                    for (ConsumerRecord<byte[], byte[]> rec : consumer.poll(Duration.ofMillis(200))) {
                        JsonNode node = mapper.readTree(rec.value());
                        deliveredByKey
                                .computeIfAbsent(node.get("key").asString(), _ -> new ArrayList<>())
                                .add(node.get("seq").asInt());
                        total++;
                    }
                }
            }

            // k1, k3, k4, k5 MUST receive exact [1,2,3,4,5,6] in strict FIFO order with ZERO duplicates
            for (String healthyKey : List.of("order-k1", "order-k3", "order-k4", "order-k5")) {
                assertThat(deliveredByKey.get(healthyKey))
                        .as("Strict FIFO delivery for %s", healthyKey)
                        .containsExactly(1, 2, 3, 4, 5, 6);
            }
            // k2 MUST stop at [1, 2]
            assertThat(deliveredByKey.get("order-k2")).containsExactly(1, 2);

            // k2 events 4, 5, 6 MUST be marked BLOCKED_BY_PRIOR_KEY_FAILURE
            var blocked = msgRepo.listMessages(dltTopic, null, MessageStatus.BLOCKED_BY_PRIOR_KEY_FAILURE, null, 20).items();
            assertThat(blocked).hasSize(3);
            assertThat(auditRepo.verifyIntegrity().valid()).isTrue();
        }
    }

    private static KeyStripedReplayExecutor createExecutor(
            String podId,
            AdminClient admin,
            JdbcDltMessageRepository msgRepo,
            JdbcReplayPlanRepository planRepo,
            JdbcStripeLeaseStore stripeStore,
            JdbcAuditLedgerRepository auditRepo,
            JsonMapper mapper,
            Clock clock) {
        DltDeckProperties props = new DltDeckProperties();
        props.setPodId(podId);
        props.getReplay().setStripeCount(4);
        props.getReplay().setMaxConcurrentStripes(2);
        props.getReplay().setMerkleCheckpointBatchSize(10);

        KafkaTemplate<byte[], byte[]> kt = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SharedContainers.KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)));
        return new KeyStripedReplayExecutor(
                kt,
                msgRepo,
                planRepo,
                stripeStore,
                auditRepo,
                new LocalRateLimitCoordinator(clock),
                new TargetTopicValidator(admin, true),
                new JsonPayloadPatcher(mapper, 1_048_576, 32),
                new ReplayHeaderSanitizer(clock),
                new MakerCheckerPolicy(),
                new BlastRadiusGuard(clock, 10, 0.5, Duration.ofMinutes(5)),
                new AuditHashChain(),
                props,
                clock);
    }
}
