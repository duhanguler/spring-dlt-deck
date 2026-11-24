package io.github.duhanguler.dltdeck.storage.jdbc.repository;

import io.github.duhanguler.dltdeck.core.model.PlanStripeLease;
import io.github.duhanguler.dltdeck.core.policy.AuditHashChain;
import io.github.duhanguler.dltdeck.core.spi.StripeLeaseStore;
import io.github.duhanguler.dltdeck.storage.jdbc.migration.DltDeckDatabaseDialect;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Deterministic multi-pod stripe lease store using {@code FOR UPDATE SKIP LOCKED} on PostgreSQL
 * to guarantee at most one pod owns any stripe {@code S} at a time.
 */
public final class JdbcStripeLeaseStore implements StripeLeaseStore {

    private final JdbcClient jdbcClient;
    private final TransactionTemplate txTemplate;
    private final DltDeckDatabaseDialect dialect;

    public JdbcStripeLeaseStore(DataSource dataSource, DltDeckDatabaseDialect dialect) {
        this.jdbcClient = JdbcClient.create(dataSource);
        this.txTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        this.dialect = dialect;
    }

    @Override
    public void initializeStripes(UUID planId, int stripeCount) {
        if (stripeCount <= 0) {
            throw new IllegalArgumentException("stripeCount must be positive");
        }
        txTemplate.executeWithoutResult(_ -> {
            String sql = dialect == DltDeckDatabaseDialect.POSTGRESQL
                    ? """
                      INSERT INTO dlt_deck_plan_stripes (plan_id, stripe_id, fencing_token, status)
                      VALUES (:planId, :stripeId, 0, 'PENDING')
                      ON CONFLICT (plan_id, stripe_id) DO NOTHING
                      """
                    : """
                      MERGE INTO dlt_deck_plan_stripes (plan_id, stripe_id, fencing_token, status)
                      KEY (plan_id, stripe_id) VALUES (:planId, :stripeId, 0, 'PENDING')
                      """;
            for (int s = 0; s < stripeCount; s++) {
                jdbcClient.sql(sql).param("planId", planId).param("stripeId", s).update();
            }
        });
    }

    @Override
    public Optional<PlanStripeLease> acquireNextStripe(
            UUID planId, String leaseOwner, Instant leaseExpiresAt, Instant now) {
        Timestamp nowTs = Timestamp.from(AuditHashChain.normalizeTimestamp(now));
        Timestamp expTs = Timestamp.from(AuditHashChain.normalizeTimestamp(leaseExpiresAt));

        return txTemplate.execute(_ -> {
            String selectSql = dialect == DltDeckDatabaseDialect.POSTGRESQL
                    ? """
                      SELECT * FROM dlt_deck_plan_stripes
                      WHERE plan_id = :planId
                        AND (status = 'PENDING' OR (status = 'ACTIVE' AND lease_expires_at < :now))
                      ORDER BY stripe_id ASC
                      FOR UPDATE SKIP LOCKED
                      LIMIT 1
                      """
                    : """
                      SELECT * FROM dlt_deck_plan_stripes
                      WHERE plan_id = :planId
                        AND (status = 'PENDING' OR (status = 'ACTIVE' AND lease_expires_at < :now))
                      ORDER BY stripe_id ASC
                      LIMIT 1
                      FOR UPDATE
                      """;
            Optional<PlanStripeLease> candidate = jdbcClient
                    .sql(selectSql)
                    .param("planId", planId)
                    .param("now", nowTs)
                    .query(JdbcStripeLeaseStore::mapStripe)
                    .optional();
            if (candidate.isEmpty()) {
                return Optional.empty();
            }
            PlanStripeLease stripe = candidate.get();
            long nextToken = stripe.fencingToken() + 1;
            int updated = jdbcClient
                    .sql("""
                            UPDATE dlt_deck_plan_stripes
                            SET status = 'ACTIVE',
                                lease_owner = :owner,
                                lease_expires_at = :exp,
                                fencing_token = :nextToken
                            WHERE plan_id = :planId
                              AND stripe_id = :stripeId
                              AND fencing_token = :prevToken
                            """)
                    .param("owner", leaseOwner)
                    .param("exp", expTs)
                    .param("nextToken", nextToken)
                    .param("planId", planId)
                    .param("stripeId", stripe.stripeId())
                    .param("prevToken", stripe.fencingToken())
                    .update();
            if (updated == 0) {
                return Optional.empty();
            }
            return Optional.of(new PlanStripeLease(
                    planId,
                    stripe.stripeId(),
                    leaseOwner,
                    AuditHashChain.normalizeTimestamp(leaseExpiresAt),
                    nextToken,
                    PlanStripeLease.StripeStatus.ACTIVE));
        });
    }

    @Override
    public boolean renewLease(
            UUID planId, int stripeId, String leaseOwner, long fencingToken, Instant newExpiry) {
        Boolean ok = txTemplate.execute(_ -> jdbcClient
                        .sql("""
                                UPDATE dlt_deck_plan_stripes
                                SET lease_expires_at = :exp
                                WHERE plan_id = :planId
                                  AND stripe_id = :stripeId
                                  AND lease_owner = :owner
                                  AND fencing_token = :token
                                  AND status = 'ACTIVE'
                                """)
                        .param("exp", Timestamp.from(AuditHashChain.normalizeTimestamp(newExpiry)))
                        .param("planId", planId)
                        .param("stripeId", stripeId)
                        .param("owner", leaseOwner)
                        .param("token", fencingToken)
                        .update()
                > 0);
        return Boolean.TRUE.equals(ok);
    }

    @Override
    public void markCompleted(UUID planId, int stripeId, String leaseOwner, long fencingToken) {
        txTemplate.executeWithoutResult(_ -> jdbcClient
                .sql("""
                        UPDATE dlt_deck_plan_stripes
                        SET status = 'COMPLETED',
                            lease_expires_at = NULL
                        WHERE plan_id = :planId
                          AND stripe_id = :stripeId
                          AND lease_owner = :owner
                          AND fencing_token = :token
                        """)
                .param("planId", planId)
                .param("stripeId", stripeId)
                .param("owner", leaseOwner)
                .param("token", fencingToken)
                .update());
    }

    @Override
    public void pauseAllStripes(UUID planId) {
        txTemplate.executeWithoutResult(_ -> jdbcClient
                .sql("""
                        UPDATE dlt_deck_plan_stripes
                        SET status = 'PAUSED',
                            lease_expires_at = NULL
                        WHERE plan_id = :planId AND status IN ('PENDING', 'ACTIVE')
                        """)
                .param("planId", planId)
                .update());
    }

    @Override
    public List<PlanStripeLease> findStripes(UUID planId) {
        return jdbcClient
                .sql("SELECT * FROM dlt_deck_plan_stripes WHERE plan_id = :planId ORDER BY stripe_id ASC")
                .param("planId", planId)
                .query(JdbcStripeLeaseStore::mapStripe)
                .list();
    }

    private static PlanStripeLease mapStripe(ResultSet rs, int rowNum) throws SQLException {
        return new PlanStripeLease(
                JdbcDltMessageRepository.readUuid(rs, "plan_id"),
                rs.getInt("stripe_id"),
                rs.getString("lease_owner"),
                JdbcDltMessageRepository.readNullableInstant(rs, "lease_expires_at"),
                rs.getLong("fencing_token"),
                PlanStripeLease.StripeStatus.valueOf(rs.getString("status")));
    }
}
