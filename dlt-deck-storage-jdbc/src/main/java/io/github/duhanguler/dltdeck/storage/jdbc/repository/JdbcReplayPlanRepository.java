package io.github.duhanguler.dltdeck.storage.jdbc.repository;

import io.github.duhanguler.dltdeck.core.model.ReplayOrderingMode;
import io.github.duhanguler.dltdeck.core.model.ReplayPlan;
import io.github.duhanguler.dltdeck.core.model.ReplayPlanStatus;
import io.github.duhanguler.dltdeck.core.policy.AuditHashChain;
import io.github.duhanguler.dltdeck.core.spi.ReplayPlanRepository;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public final class JdbcReplayPlanRepository implements ReplayPlanRepository {

    private final JdbcClient jdbcClient;
    private final TransactionTemplate txTemplate;

    public JdbcReplayPlanRepository(DataSource dataSource) {
        this.jdbcClient = JdbcClient.create(dataSource);
        this.txTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    @Override
    public ReplayPlan save(ReplayPlan plan) {
        txTemplate.executeWithoutResult(_ -> jdbcClient
                .sql("""
                        INSERT INTO dlt_deck_replay_plans (
                            id, name, fingerprint_filter, dlt_topic_filter, target_topic,
                            ordering_mode, payload_patch_json, rate_limit_per_sec, max_records,
                            snapshot_count, plan_spec_hash, status, requires_approval,
                            created_by, approved_by, rejection_reason,
                            total_claimed, total_published, total_failed,
                            total_indeterminate, total_blocked_hol, total_re_dlt,
                            created_at, approved_at, completed_at
                        ) VALUES (
                            :id, :name, :fpFilter, :topicFilter, :targetTopic,
                            :orderingMode, :patchJson, :rateLimit, :maxRecords,
                            :snapshotCount, :specHash, :status, :requiresApproval,
                            :createdBy, :approvedBy, :rejectionReason,
                            :claimed, :published, :failed, :indeterminate, :blockedHol, :reDlt,
                            :createdAt, :approvedAt, :completedAt
                        )
                        """)
                .param("id", plan.id())
                .param("name", plan.name())
                .param("fpFilter", plan.fingerprintFilter())
                .param("topicFilter", plan.dltTopicFilter())
                .param("targetTopic", plan.targetTopic())
                .param("orderingMode", plan.orderingMode().name())
                .param("patchJson", plan.payloadPatchJson())
                .param("rateLimit", plan.rateLimitPerSec())
                .param("maxRecords", plan.maxRecords())
                .param("snapshotCount", plan.snapshotCount())
                .param("specHash", plan.planSpecHash())
                .param("status", plan.status().name())
                .param("requiresApproval", plan.requiresApproval())
                .param("createdBy", plan.createdBy())
                .param("approvedBy", plan.approvedBy())
                .param("rejectionReason", plan.rejectionReason())
                .param("claimed", plan.totalClaimed())
                .param("published", plan.totalPublished())
                .param("failed", plan.totalFailed())
                .param("indeterminate", plan.totalIndeterminate())
                .param("blockedHol", plan.totalBlockedHol())
                .param("reDlt", plan.totalReDlt())
                .param("createdAt", Timestamp.from(AuditHashChain.normalizeTimestamp(plan.createdAt())))
                .param("approvedAt", toNullableTs(plan.approvedAt()))
                .param("completedAt", toNullableTs(plan.completedAt()))
                .update());
        return plan;
    }

    @Override
    public Optional<ReplayPlan> findById(UUID planId) {
        return jdbcClient
                .sql("SELECT * FROM dlt_deck_replay_plans WHERE id = :id")
                .param("id", planId)
                .query(JdbcReplayPlanRepository::mapPlan)
                .optional();
    }

    @Override
    public List<ReplayPlan> listRecent(int limit) {
        return jdbcClient
                .sql("SELECT * FROM dlt_deck_replay_plans ORDER BY created_at DESC LIMIT :limit")
                .param("limit", Math.clamp(limit, 1, 200))
                .query(JdbcReplayPlanRepository::mapPlan)
                .list();
    }

    @Override
    public List<ReplayPlan> findByStatus(ReplayPlanStatus status) {
        return jdbcClient
                .sql("SELECT * FROM dlt_deck_replay_plans WHERE status = :status ORDER BY created_at ASC")
                .param("status", status.name())
                .query(JdbcReplayPlanRepository::mapPlan)
                .list();
    }

    @Override
    public boolean approve(UUID planId, String approvedBy, Instant approvedAt) {
        Boolean ok = txTemplate.execute(_ -> jdbcClient
                        .sql("""
                                UPDATE dlt_deck_replay_plans
                                SET status = 'APPROVED',
                                    approved_by = :approvedBy,
                                    approved_at = :approvedAt
                                WHERE id = :id AND status = 'PENDING_APPROVAL'
                                """)
                        .param("approvedBy", approvedBy)
                        .param("approvedAt", Timestamp.from(AuditHashChain.normalizeTimestamp(approvedAt)))
                        .param("id", planId)
                        .update()
                > 0);
        return Boolean.TRUE.equals(ok);
    }

    @Override
    public boolean reject(UUID planId, String rejectedBy, String reason, Instant rejectedAt) {
        Boolean ok = txTemplate.execute(_ -> jdbcClient
                        .sql("""
                                UPDATE dlt_deck_replay_plans
                                SET status = 'REJECTED',
                                    approved_by = :rejectedBy,
                                    rejection_reason = :reason,
                                    completed_at = :rejectedAt
                                WHERE id = :id AND status = 'PENDING_APPROVAL'
                                """)
                        .param("rejectedBy", rejectedBy)
                        .param("reason", reason)
                        .param("rejectedAt", Timestamp.from(AuditHashChain.normalizeTimestamp(rejectedAt)))
                        .param("id", planId)
                        .update()
                > 0);
        return Boolean.TRUE.equals(ok);
    }

    @Override
    public boolean updateStatus(
            UUID planId,
            ReplayPlanStatus expectedCurrent,
            ReplayPlanStatus newStatus,
            @Nullable Instant completedAt) {
        Boolean ok = txTemplate.execute(_ -> jdbcClient
                        .sql("""
                                UPDATE dlt_deck_replay_plans
                                SET status = :newStatus,
                                    completed_at = COALESCE(:completedAt, completed_at)
                                WHERE id = :id AND status = :expectedCurrent
                                """)
                        .param("newStatus", newStatus.name())
                        .param("completedAt", toNullableTs(completedAt))
                        .param("id", planId)
                        .param("expectedCurrent", expectedCurrent.name())
                        .update()
                > 0);
        return Boolean.TRUE.equals(ok);
    }

    @Override
    public void incrementCounters(
            UUID planId,
            int claimedDelta,
            int publishedDelta,
            int failedDelta,
            int indeterminateDelta,
            int blockedHolDelta,
            int reDltDelta) {
        txTemplate.executeWithoutResult(_ -> jdbcClient
                .sql("""
                        UPDATE dlt_deck_replay_plans
                        SET total_claimed = total_claimed + :claimed,
                            total_published = total_published + :published,
                            total_failed = total_failed + :failed,
                            total_indeterminate = total_indeterminate + :indeterminate,
                            total_blocked_hol = total_blocked_hol + :blockedHol,
                            total_re_dlt = total_re_dlt + :reDlt
                        WHERE id = :id
                        """)
                .param("claimed", claimedDelta)
                .param("published", publishedDelta)
                .param("failed", failedDelta)
                .param("indeterminate", indeterminateDelta)
                .param("blockedHol", blockedHolDelta)
                .param("reDlt", reDltDelta)
                .param("id", planId)
                .update());
    }

    private static @Nullable Timestamp toNullableTs(@Nullable Instant instant) {
        return instant == null ? null : Timestamp.from(AuditHashChain.normalizeTimestamp(instant));
    }

    private static ReplayPlan mapPlan(ResultSet rs, int rowNum) throws SQLException {
        return new ReplayPlan(
                JdbcDltMessageRepository.readUuid(rs, "id"),
                rs.getString("name"),
                rs.getString("fingerprint_filter"),
                rs.getString("dlt_topic_filter"),
                rs.getString("target_topic"),
                ReplayOrderingMode.valueOf(rs.getString("ordering_mode")),
                rs.getString("payload_patch_json"),
                rs.getInt("rate_limit_per_sec"),
                rs.getInt("max_records"),
                rs.getInt("snapshot_count"),
                rs.getString("plan_spec_hash"),
                ReplayPlanStatus.valueOf(rs.getString("status")),
                rs.getBoolean("requires_approval"),
                rs.getString("created_by"),
                rs.getString("approved_by"),
                rs.getString("rejection_reason"),
                rs.getInt("total_claimed"),
                rs.getInt("total_published"),
                rs.getInt("total_failed"),
                rs.getInt("total_indeterminate"),
                rs.getInt("total_blocked_hol"),
                rs.getInt("total_re_dlt"),
                JdbcDltMessageRepository.readInstant(rs, "created_at"),
                JdbcDltMessageRepository.readNullableInstant(rs, "approved_at"),
                JdbcDltMessageRepository.readNullableInstant(rs, "completed_at"));
    }
}
