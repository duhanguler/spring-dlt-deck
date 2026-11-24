package io.github.duhanguler.dltdeck.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.duhanguler.dltdeck.core.model.FailureCategory;
import io.github.duhanguler.dltdeck.core.model.IngestedDltMessage;
import io.github.duhanguler.dltdeck.core.model.MessageStatus;
import io.github.duhanguler.dltdeck.core.model.PayloadFormat;
import io.github.duhanguler.dltdeck.core.model.PlanStripeLease;
import io.github.duhanguler.dltdeck.core.model.ReplayOrderingMode;
import io.github.duhanguler.dltdeck.core.model.ReplayPlan;
import io.github.duhanguler.dltdeck.core.model.ReplayPlanStatus;
import io.github.duhanguler.dltdeck.core.policy.AuditHashChain;
import io.github.duhanguler.dltdeck.core.policy.KeysetCursorCodec;
import io.github.duhanguler.dltdeck.core.policy.MakerCheckerPolicy;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class JdbcStorageH2Test {

    private HikariDataSource ds;
    private JdbcDltMessageRepository messageRepo;
    private JdbcReplayPlanRepository planRepo;
    private JdbcStripeLeaseStore stripeStore;
    private JdbcAuditLedgerRepository auditRepo;
    private Instant now;

    @BeforeEach
    void setUp() {
        now = Instant.parse("2026-11-24T12:00:00Z");
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl("jdbc:h2:mem:dltdeck_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        cfg.setUsername("sa");
        cfg.setPassword("");
        ds = new HikariDataSource(cfg);

        DltDeckSchemaMigrator migrator = new DltDeckSchemaMigrator(ds, Clock.fixed(now, ZoneOffset.UTC));
        assertThat(migrator.dialect()).isEqualTo(DltDeckDatabaseDialect.H2);
        migrator.migrate();
        migrator.migrate(); // idempotent re-run check

        JsonMapper mapper = JsonMapper.builder().build();
        PayloadStorageCodec storageCodec = new PayloadStorageCodec(1_048_576, 256, true, null);
        KeysetCursorCodec cursorCodec =
                new KeysetCursorCodec("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8));
        messageRepo = new JdbcDltMessageRepository(ds, DltDeckDatabaseDialect.H2, storageCodec, cursorCodec, mapper);
        planRepo = new JdbcReplayPlanRepository(ds);
        stripeStore = new JdbcStripeLeaseStore(ds, DltDeckDatabaseDialect.H2);
        auditRepo = new JdbcAuditLedgerRepository(ds, DltDeckDatabaseDialect.H2, new AuditHashChain());
    }

    @AfterEach
    void tearDown() {
        if (ds != null) {
            ds.close();
        }
    }

    @Test
    void ingestsCompressesPaginatesClaimsFencesSweepsAndVerifiesAuditChain() {
        byte[] largeJson = ("{\"orderId\":\"ORD-1\",\"blob\":\"" + "A".repeat(2048) + "\"}")
                .getBytes(StandardCharsets.UTF_8);
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        IngestedDltMessage m1 = new IngestedDltMessage(
                id1, "orders.dlt", 0, 1L, "orders", 0, 10L, "cg", "k1".getBytes(StandardCharsets.UTF_8),
                1, "kfp-1", largeJson, PayloadFormat.JSON, null, largeJson.length,
                "fp-validation", "Ex", "Ex", "bad field", "stack", FailureCategory.VALIDATION, "fix",
                0, MessageStatus.NEW, null, Map.of("x-tenant", "tr"), now);
        IngestedDltMessage m2 = new IngestedDltMessage(
                id2, "orders.dlt", 0, 2L, "orders", 0, 11L, "cg", "k1".getBytes(StandardCharsets.UTF_8),
                1, "kfp-1", "{}".getBytes(StandardCharsets.UTF_8), PayloadFormat.JSON, null, 2,
                "fp-validation", "Ex", "Ex", "bad field", "stack", FailureCategory.VALIDATION, "fix",
                0, MessageStatus.NEW, null, Map.of(), now.plusSeconds(1));

        assertThat(messageRepo.ingestBatch(List.of(m1, m2))).isEqualTo(2);
        assertThat(messageRepo.ingestBatch(List.of(m1, m2))).isZero(); // idempotent coordinates

        var clusters = messageRepo.listClusters("orders.dlt", 10);
        assertThat(clusters).hasSize(1);
        assertThat(clusters.getFirst().totalCount()).isEqualTo(2);

        // Keyset pagination
        var p1 = messageRepo.listMessages("orders.dlt", "fp-validation", MessageStatus.NEW, null, 1);
        assertThat(p1.items()).hasSize(1);
        assertThat(p1.hasMore()).isTrue();
        var p2 = messageRepo.listMessages("orders.dlt", "fp-validation", MessageStatus.NEW, p1.nextCursor(), 1);
        assertThat(p2.items()).hasSize(1);
        assertThat(p2.hasMore()).isFalse();

        // GZIP round-trip decompression check
        var detail = messageRepo.findDetailById(id1).orElseThrow();
        assertThat(detail.payloadEncoding()).isEqualTo(PayloadStorageCodec.ENCODING_GZIP);
        assertThat(detail.payload()).isEqualTo(largeJson);

        // Plan + Stripe leasing + TX1/TX2 fencing + INDETERMINATE sweep
        UUID planId = UUID.randomUUID();
        String specHash = new MakerCheckerPolicy()
                .computePlanSpecHash("p", "fp-validation", "orders.dlt", "orders", ReplayOrderingMode.PRESERVE_KEY_ORDER, null, 10, 100, 2);
        planRepo.save(new ReplayPlan(
                planId, "p", "fp-validation", "orders.dlt", "orders", ReplayOrderingMode.PRESERVE_KEY_ORDER, null,
                10, 100, 2, specHash, ReplayPlanStatus.PENDING_APPROVAL, true, "op", null, null,
                0, 0, 0, 0, 0, 0, now, null, null));
        assertThat(planRepo.approve(planId, "approver", now.plusSeconds(5))).isTrue();

        stripeStore.initializeStripes(planId, 2);
        PlanStripeLease lease = stripeStore.acquireNextStripe(planId, "pod-1", now.plusSeconds(30), now).orElseThrow();
        assertThat(lease.stripeId()).isZero();
        assertThat(stripeStore.renewLease(planId, 0, "pod-1", lease.fencingToken(), now.plusSeconds(60))).isTrue();

        var claimed = messageRepo.claimForReplay(id1, planId, "pod-1", now.plusSeconds(10), now).orElseThrow();
        assertThat(claimed.fencingToken()).isEqualTo(1L);
        // Wrong fencing token rejected!
        assertThat(messageRepo.markPublished(id1, 999L, 0, 100L, now.plusSeconds(2))).isFalse();
        assertThat(messageRepo.markPublished(id1, 1L, 0, 100L, now.plusSeconds(2))).isTrue();

        // Claim m2 and simulate crash -> sweep expired lease to INDETERMINATE
        assertThat(messageRepo.claimForReplay(id2, planId, "pod-1", now.plusSeconds(5), now)).isPresent();
        var swept = messageRepo.sweepExpiredLeasesToIndeterminate(now.plusSeconds(20), 10);
        assertThat(swept).hasSize(1);
        assertThat(messageRepo.findDetailById(id2).orElseThrow().summary().status())
                .isEqualTo(MessageStatus.INDETERMINATE);

        // Audit hash chain + Merkle checkpoint
        auditRepo.append(UUID.randomUUID(), "op", "OPERATOR", "CREATE_PLAN", "PLAN", planId.toString(), null, "{}", now);
        auditRepo.append(
                UUID.randomUUID(), "system", "SYSTEM", "BATCH_CHECKPOINT", "PLAN", planId.toString(),
                new AuditHashChain().computeMerkleRoot(List.of(id1)), "{\"count\":1}", now.plusSeconds(2));
        assertThat(auditRepo.verifyIntegrity().valid()).isTrue();
        assertThat(auditRepo.verifyIntegrity().totalChecked()).isEqualTo(2);

        // Retention sweep
        assertThat(messageRepo.purgeOlderThan(now.plus(Duration.ofDays(1)), 100)).isEqualTo(2);
    }
}
