package io.github.duhanguler.dltdeck.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.duhanguler.dltdeck.core.codec.DefaultPayloadCodec;
import io.github.duhanguler.dltdeck.core.fingerprint.ExceptionFingerprinter;
import io.github.duhanguler.dltdeck.core.masking.PiiRedactor;
import io.github.duhanguler.dltdeck.core.model.FailureCategory;
import io.github.duhanguler.dltdeck.core.model.IngestedDltMessage;
import io.github.duhanguler.dltdeck.core.model.MessageStatus;
import io.github.duhanguler.dltdeck.core.model.PayloadFormat;
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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

class DltDeckMcpServiceTest {

    @Test
    void mcpAlwaysRedactsPiiRunsTier1DryRunForcesPendingApprovalAndRejectsUnknownTools() {
        Instant now = Instant.parse("2026-11-24T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl("jdbc:h2:mem:mcp_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        hc.setUsername("sa");
        hc.setPassword("");

        try (HikariDataSource ds = new HikariDataSource(hc);
                AdminClient admin = AdminClient.create(
                        Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092"))) {
            new DltDeckSchemaMigrator(ds, clock).migrate();
            JsonMapper mapper = JsonMapper.builder().build();
            DltDeckProperties props = new DltDeckProperties();
            props.getReplay().setRequireFourEyesApproval(false); // Even if disabled, MCP MUST force PENDING_APPROVAL!
            props.getReplay().setValidateTargetTopicExists(false);

            var msgRepo = new JdbcDltMessageRepository(
                    ds, DltDeckDatabaseDialect.H2,
                    new PayloadStorageCodec(1_048_576, 4096, true, null),
                    new KeysetCursorCodec("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8)), mapper);
            var planRepo = new JdbcReplayPlanRepository(ds);
            var stripeStore = new JdbcStripeLeaseStore(ds, DltDeckDatabaseDialect.H2);
            var auditRepo = new JdbcAuditLedgerRepository(ds, DltDeckDatabaseDialect.H2, new AuditHashChain());
            var blastGuard = new BlastRadiusGuard(clock, 10, 0.5, Duration.ofMinutes(5));
            var validator = new TargetTopicValidator(admin, false);
            var kt = new KafkaTemplate<byte[], byte[]>(new DefaultKafkaProducerFactory<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092",
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class)));
            var patcher = new JsonPayloadPatcher(mapper, 1_048_576, 32);
            var codec = new DefaultPayloadCodec(mapper);

            var service = new DltDeckControlPlaneService(
                    msgRepo, planRepo, auditRepo,
                    new DltBatchIngestionEngine(
                            Map.of(), admin, props, new SafeSpringHeaderExtractor(), codec,
                            new ExceptionFingerprinter(), msgRepo, planRepo, stripeStore, blastGuard, clock),
                    new KeyStripedReplayExecutor(
                            kt, msgRepo, planRepo, stripeStore, auditRepo, new LocalRateLimitCoordinator(clock),
                            validator, patcher, new ReplayHeaderSanitizer(clock), new MakerCheckerPolicy(),
                            blastGuard, new AuditHashChain(), props, clock),
                    new TieredDryRunService(
                            msgRepo, patcher, codec, mapper, null, List.of(), validator, new ReplayHeaderSanitizer(clock), kt),
                    validator, codec, new PiiRedactor(), patcher, new MakerCheckerPolicy(), props, mapper, clock);

            UUID msgId = UUID.randomUUID();
            byte[] payload = "{\"tckn\":\"10000000146\",\"currency\":\"TRL\"}".getBytes(StandardCharsets.UTF_8);
            msgRepo.ingestBatch(List.of(new IngestedDltMessage(
                    msgId, "orders.dlt", 0, 1L, "orders", 0, 1L, "cg", "k1".getBytes(StandardCharsets.UTF_8),
                    1, "kfp1", payload, PayloadFormat.JSON, null, payload.length,
                    "fp-mcp", "ValidationException", "ValidationException", "bad currency", "stack",
                    FailureCategory.VALIDATION, "patch", 0, MessageStatus.NEW, null, Map.of(), now)));

            DltDeckMcpService mcp = new DltDeckMcpService(service);
            DltDeckMcpToolRegistry registry = new DltDeckMcpToolRegistry(mcp, mapper);

            assertThat(registry.listTools()).hasSize(4);

            // 1. Inspect ALWAYS redacts PII
            var inspected = mcp.inspectDltMessage(msgId);
            assertThat(inspected.piiRedacted()).isTrue();
            assertThat(inspected.payloadPreview()).doesNotContain("10000000146").contains("[REDACTED_SECRET]");

            // 2. Dry-run executes Tier-1 cleanly
            var dry = mcp.dryRunDltPatch(msgId, "{\"currency\":\"TRY\"}");
            assertThat(dry.passed()).isTrue();

            // 3. Draft plan ALWAYS forces PENDING_APPROVAL even when requireFourEyesApproval=false!
            ReplayPlan plan = mcp.draftReplayPlan("AI Suggested Recovery", "orders.dlt", "fp-mcp", "orders", "{\"currency\":\"TRY\"}", 25, 50);
            assertThat(plan.status()).isEqualTo(ReplayPlanStatus.PENDING_APPROVAL);
            assertThat(plan.requiresApproval()).isTrue();
            assertThat(plan.createdBy()).isEqualTo("mcp-agent");

            // 4. JSON-RPC dispatch + fail-closed rejection of unknown/approval tool names
            var listResp = registry.handleJsonRpc(Map.of("id", 1, "method", "tools/list"));
            assertThat(listResp).containsKey("result");
            assertThatThrownBy(() -> registry.invokeTool("approve_replay_plan", Map.of()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
