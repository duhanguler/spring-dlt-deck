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
import io.github.duhanguler.dltdeck.core.model.FailureCategory;
import io.github.duhanguler.dltdeck.core.model.MessageStatus;
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
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import tools.jackson.databind.json.JsonMapper;

/**
 * Verifies target topic validation guard ({@code .dlt} and non-existent typo topics rejected via AdminClient),
 * replay loop quarantine at {@code maxReplayHops = 3}, blast-radius auto-pause ({@code PAUSED_BLAST_RADIUS}),
 * and clean {@code SmartLifecycle.stop()} graceful shutdown drain.
 */
class BlastRadiusLoopAndTargetTopicGuardIT {

    @Test
    void guardsTargetTopicsQuarantinesReplayLoopsTripsBlastRadiusAndDrainsGracefully() throws Exception {
        String dltTopic = "guard-orders.dlt";
        String targetTopic = "guard-orders-" + UUID.randomUUID().toString().substring(0, 8);
        SharedContainers.createTopics(Map.of(dltTopic, 1, targetTopic, 1));

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
            props.getIngestion().setMaxReplayHops(3);
            props.getIngestion().setGroupId("dlt-deck-guard-it-" + System.nanoTime());

            var msgRepo = new JdbcDltMessageRepository(
                    ds, DltDeckDatabaseDialect.POSTGRESQL,
                    new PayloadStorageCodec(1_048_576, 4096, true, null),
                    new KeysetCursorCodec("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8)), mapper);
            var planRepo = new JdbcReplayPlanRepository(ds);
            var stripeStore = new JdbcStripeLeaseStore(ds, DltDeckDatabaseDialect.POSTGRESQL);
            var auditRepo = new JdbcAuditLedgerRepository(ds, DltDeckDatabaseDialect.POSTGRESQL, new AuditHashChain());
            var blastGuard = new BlastRadiusGuard(clock, 2, 0.5, Duration.ofMinutes(5));
            var validator = new TargetTopicValidator(admin, true);
            var kafkaTemplate = new KafkaTemplate<byte[], byte[]>(new DefaultKafkaProducerFactory<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SharedContainers.KAFKA.getBootstrapServers(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class)));

            var engine = new DltBatchIngestionEngine(
                    Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, SharedContainers.KAFKA.getBootstrapServers()),
                    admin, props, new SafeSpringHeaderExtractor(), new DefaultPayloadCodec(mapper),
                    new ExceptionFingerprinter(), msgRepo, planRepo, stripeStore, blastGuard, clock);
            var executor = new KeyStripedReplayExecutor(
                    kafkaTemplate, msgRepo, planRepo, stripeStore, auditRepo, new LocalRateLimitCoordinator(clock),
                    validator, new JsonPayloadPatcher(mapper, 1_048_576, 32), new ReplayHeaderSanitizer(clock),
                    new MakerCheckerPolicy(), blastGuard, new AuditHashChain(), props, clock);
            var service = new DltDeckControlPlaneService(
                    msgRepo, planRepo, auditRepo, engine, executor,
                    new TieredDryRunService(msgRepo, new JsonPayloadPatcher(mapper, 1_048_576, 32),
                            new DefaultPayloadCodec(mapper), mapper, null, List.of(), validator, new ReplayHeaderSanitizer(clock), kafkaTemplate),
                    validator, new DefaultPayloadCodec(mapper), new PiiRedactor(),
                    new JsonPayloadPatcher(mapper, 1_048_576, 32), new MakerCheckerPolicy(), props, mapper, clock);

            // 1. TargetTopicValidator blocks .dlt target and non-existent typo topic
            ActorContext.runAs(new ActorContext("op", ActorRole.OPERATOR, null), () -> {
                assertThatThrownBy(() -> service.draftReplayPlan(new DltDeckControlPlaneService.DraftPlanRequest(
                                "Bad DLT Target", null, dltTopic, "orders.dlt", ReplayOrderingMode.PRESERVE_KEY_ORDER, null, 10, 10)))
                        .isInstanceOf(ValidationDomainException.class)
                        .extracting("errorCode")
                        .isEqualTo("FORBIDDEN_DLT_TARGET");

                assertThatThrownBy(() -> service.draftReplayPlan(new DltDeckControlPlaneService.DraftPlanRequest(
                                "Typo Topic", null, dltTopic, "typo-nonexistent-topic-xyz", ReplayOrderingMode.PRESERVE_KEY_ORDER, null, 10, 10)))
                        .isInstanceOf(ValidationDomainException.class)
                        .extracting("errorCode")
                        .isEqualTo("TARGET_TOPIC_NOT_FOUND");
            });

            // 2. Seed a RUNNING replay plan so correlated re-DLTs trip PAUSED_BLAST_RADIUS
            UUID planId = UUID.randomUUID();
            String specHash = new MakerCheckerPolicy()
                    .computePlanSpecHash("Blast Test", null, dltTopic, targetTopic, ReplayOrderingMode.PRESERVE_KEY_ORDER, null, 10, 10, 2);
            planRepo.save(new ReplayPlan(
                    planId, "Blast Test", null, dltTopic, targetTopic, ReplayOrderingMode.PRESERVE_KEY_ORDER, null,
                    10, 10, 2, specHash, ReplayPlanStatus.RUNNING, true, "op", "approver", null,
                    0, 0, 0, 0, 0, 0, clock.instant(), clock.instant(), null));
            stripeStore.initializeStripes(planId, 2);

            // Publish 2 correlated re-DLT records (one also at replayHops=3 -> QUARANTINED!)
            for (int i = 0; i < 2; i++) {
                ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(
                        dltTopic, ("k-" + i).getBytes(StandardCharsets.UTF_8), "{\"a\":1}".getBytes(StandardCharsets.UTF_8));
                rec.headers().add(new RecordHeader("x-dlt-deck-replay-plan-id", planId.toString().getBytes(StandardCharsets.UTF_8)));
                rec.headers().add(new RecordHeader("x-dlt-deck-replay-hops", (i == 0 ? "3" : "1").getBytes(StandardCharsets.UTF_8)));
                producer.send(rec).get();
            }
            producer.flush();

            // 3. Start SmartLifecycle engines, wait for ingestion, then cleanly stop()
            engine.start();
            executor.start();
            assertThat(engine.isRunning()).isTrue();
            assertThat(executor.isRunning()).isTrue();
            try {
                long deadline = System.currentTimeMillis() + 20_000;
                while (System.currentTimeMillis() < deadline) {
                    if (planRepo.findById(planId).orElseThrow().status() == ReplayPlanStatus.PAUSED_BLAST_RADIUS) {
                        break;
                    }
                    Thread.sleep(200);
                }
            } finally {
                engine.stop();
                executor.stop();
            }
            assertThat(engine.isRunning()).isFalse();
            assertThat(executor.isRunning()).isFalse();

            // Verify Blast-Radius tripped and hop-3 record was QUARANTINED
            assertThat(planRepo.findById(planId).orElseThrow().status()).isEqualTo(ReplayPlanStatus.PAUSED_BLAST_RADIUS);
            var quarantined = msgRepo.listMessages(dltTopic, null, MessageStatus.QUARANTINED, null, 10).items();
            assertThat(quarantined).hasSize(1);
            assertThat(quarantined.getFirst().failureCategory()).isEqualTo(FailureCategory.REPLAY_LOOP);
        }
    }
}
