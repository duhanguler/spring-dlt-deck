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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers
class JdbcStoragePostgresIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    @Test
    void runsAdvisoryLockedMigrationsStripeSkipLockedAndMicrosecondExactAuditOnPostgres17() throws Exception {
        Instant now = Instant.parse("2026-11-24T12:34:56.789123456Z");
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(POSTGRES.getJdbcUrl());
        cfg.setUsername(POSTGRES.getUsername());
        cfg.setPassword(POSTGRES.getPassword());
        cfg.setMaximumPoolSize(8);

        try (HikariDataSource ds = new HikariDataSource(cfg)) {
            DltDeckSchemaMigrator migrator = new DltDeckSchemaMigrator(ds, Clock.fixed(now, ZoneOffset.UTC));
            assertThat(migrator.dialect()).isEqualTo(DltDeckDatabaseDialect.POSTGRESQL);

            // Concurrent multi-pod startup migration guarded by pg_advisory_lock
            try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Callable<Void>> tasks = List.of(
                        () -> {
                            migrator.migrate();
                            return null;
                        },
                        () -> {
                            migrator.migrate();
                            return null;
                        },
                        () -> {
                            migrator.migrate();
                            return null;
                        });
                for (var f : exec.invokeAll(tasks)) {
                    f.get();
                }
            }

            JsonMapper mapper = JsonMapper.builder().build();
            PayloadStorageCodec storageCodec = new PayloadStorageCodec(1_048_576, 512, true, null);
            KeysetCursorCodec cursorCodec =
                    new KeysetCursorCodec("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8));
            JdbcDltMessageRepository msgRepo =
                    new JdbcDltMessageRepository(ds, DltDeckDatabaseDialect.POSTGRESQL, storageCodec, cursorCodec, mapper);
            JdbcReplayPlanRepository planRepo = new JdbcReplayPlanRepository(ds);
            JdbcStripeLeaseStore stripeStore = new JdbcStripeLeaseStore(ds, DltDeckDatabaseDialect.POSTGRESQL);
            JdbcAuditLedgerRepository auditRepo =
                    new JdbcAuditLedgerRepository(ds, DltDeckDatabaseDialect.POSTGRESQL, new AuditHashChain());

            UUID m1 = UUID.randomUUID();
            UUID m2 = UUID.randomUUID();
            IngestedDltMessage d1 = new IngestedDltMessage(
                    m1, "payments.dlt", 0, 10L, "payments", 0, 1L, "cg", "key-a".getBytes(StandardCharsets.UTF_8),
                    10, "kfp-a", "{\"seq\":1}".getBytes(StandardCharsets.UTF_8), PayloadFormat.JSON, null, 9,
                    "fp-pg", "Ex", "Ex", "err", "stack", FailureCategory.TRANSIENT_DOWNSTREAM, "retry",
                    0, MessageStatus.NEW, null, Map.of("traceparent", "00-1-2-01"), now);
            IngestedDltMessage d2 = new IngestedDltMessage(
                    m2, "payments.dlt", 0, 11L, "payments", 0, 2L, "cg", "key-a".getBytes(StandardCharsets.UTF_8),
                    10, "kfp-a", "{\"seq\":2}".getBytes(StandardCharsets.UTF_8), PayloadFormat.JSON, null, 9,
                    "fp-pg", "Ex", "Ex", "err", "stack", FailureCategory.TRANSIENT_DOWNSTREAM, "retry",
                    0, MessageStatus.NEW, null, Map.of(), now.plusSeconds(1));

            assertThat(msgRepo.ingestBatch(List.of(d1, d2))).isEqualTo(2);
            assertThat(msgRepo.ingestBatch(List.of(d1, d2))).isZero(); // ON CONFLICT DO NOTHING

            UUID planId = UUID.randomUUID();
            String specHash = new MakerCheckerPolicy()
                    .computePlanSpecHash("pg-plan", "fp-pg", "payments.dlt", "payments", ReplayOrderingMode.PRESERVE_KEY_ORDER, null, 20, 100, 2);
            planRepo.save(new ReplayPlan(
                    planId, "pg-plan", "fp-pg", "payments.dlt", "payments", ReplayOrderingMode.PRESERVE_KEY_ORDER, null,
                    20, 100, 2, specHash, ReplayPlanStatus.APPROVED, true, "op", "approver", null,
                    0, 0, 0, 0, 0, 0, now, now, null));
            stripeStore.initializeStripes(planId, 2);

            PlanStripeLease s0 = stripeStore.acquireNextStripe(planId, "pod-a", now.plusSeconds(30), now).orElseThrow();
            PlanStripeLease s1 = stripeStore.acquireNextStripe(planId, "pod-b", now.plusSeconds(30), now).orElseThrow();
            assertThat(s0.stripeId()).isNotEqualTo(s1.stripeId());
            assertThat(stripeStore.acquireNextStripe(planId, "pod-c", now.plusSeconds(30), now)).isEmpty();

            // HoL barrier check
            int blocked = msgRepo.blockSubsequentForKey(planId, "payments.dlt", "kfp-a", null, now.plusSeconds(5));
            assertThat(blocked).isEqualTo(2);

            // Microsecond-truncated TIMESTAMPTZ audit chain verification on PostgreSQL 17
            auditRepo.append(UUID.randomUUID(), "op", "OPERATOR", "DRAFT", "PLAN", planId.toString(), null, "{}", now);
            auditRepo.append(
                    UUID.randomUUID(), "approver", "APPROVER", "APPROVE", "PLAN", planId.toString(),
                    new AuditHashChain().computeMerkleRoot(List.of(m1, m2)), "{\"ok\":true}", now.plusMillis(250));
            var verification = auditRepo.verifyIntegrity();
            assertThat(verification.valid()).isTrue();
            assertThat(verification.totalChecked()).isEqualTo(2);
        }
    }
}
