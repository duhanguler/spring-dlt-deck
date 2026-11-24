package io.github.duhanguler.dltdeck.storage.jdbc.repository;

import io.github.duhanguler.dltdeck.core.model.AuditChainVerification;
import io.github.duhanguler.dltdeck.core.model.AuditEntry;
import io.github.duhanguler.dltdeck.core.policy.AuditHashChain;
import io.github.duhanguler.dltdeck.core.spi.AuditLedgerRepository;
import io.github.duhanguler.dltdeck.storage.jdbc.migration.DltDeckDatabaseDialect;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import javax.sql.DataSource;
import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Append-only cryptographic SHA-256 hash-chained audit ledger for control-plane events and
 * batch Merkle-root checkpoints. Uses PostgreSQL transaction-scoped advisory locks ONLY on
 * control-plane/checkpoint appends (never per replayed message) so replay throughput stays uncontended.
 */
public final class JdbcAuditLedgerRepository implements AuditLedgerRepository {

    private static final ReentrantLock H2_SERIALIZE_LOCK = new ReentrantLock();

    private final JdbcClient jdbcClient;
    private final TransactionTemplate txTemplate;
    private final DltDeckDatabaseDialect dialect;
    private final AuditHashChain hashChain;

    public JdbcAuditLedgerRepository(
            DataSource dataSource, DltDeckDatabaseDialect dialect, AuditHashChain hashChain) {
        this.jdbcClient = JdbcClient.create(dataSource);
        this.txTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        this.dialect = dialect;
        this.hashChain = hashChain;
    }

    @Override
    public AuditEntry append(
            UUID eventId,
            String actor,
            String actorRole,
            String action,
            String targetType,
            String targetId,
            @Nullable String merkleRoot,
            String detailsJson,
            Instant createdAt) {
        Instant normalizedTime = AuditHashChain.normalizeTimestamp(createdAt);
        if (dialect == DltDeckDatabaseDialect.H2) {
            H2_SERIALIZE_LOCK.lock();
        }
        try {
            AuditEntry saved = txTemplate.execute(_ -> {
                if (dialect == DltDeckDatabaseDialect.POSTGRESQL) {
                    jdbcClient.sql("SELECT pg_advisory_xact_lock(hashtext('dlt_deck_audit_ledger'))")
                            .query()
                            .singleValue();
                }
                Optional<String> latest = jdbcClient
                        .sql("SELECT entry_hash FROM dlt_deck_audit_ledger ORDER BY sequence_no DESC LIMIT 1")
                        .query(String.class)
                        .optional();
                String prevHash = latest.orElse(AuditHashChain.GENESIS_HASH);
                String entryHash = hashChain.computeEntryHash(
                        prevHash,
                        eventId,
                        actor,
                        actorRole,
                        action,
                        targetType,
                        targetId,
                        merkleRoot,
                        detailsJson,
                        normalizedTime);

                jdbcClient
                        .sql("""
                                INSERT INTO dlt_deck_audit_ledger (
                                    event_id, actor, actor_role, action, target_type, target_id,
                                    merkle_root, details_json, prev_hash, entry_hash, created_at
                                ) VALUES (
                                    :eventId, :actor, :actorRole, :action, :targetType, :targetId,
                                    :merkleRoot, :detailsJson, :prevHash, :entryHash, :createdAt
                                )
                                """)
                        .param("eventId", eventId)
                        .param("actor", actor)
                        .param("actorRole", actorRole)
                        .param("action", action)
                        .param("targetType", targetType)
                        .param("targetId", targetId)
                        .param("merkleRoot", merkleRoot)
                        .param("detailsJson", detailsJson)
                        .param("prevHash", prevHash)
                        .param("entryHash", entryHash)
                        .param("createdAt", Timestamp.from(normalizedTime))
                        .update();

                return jdbcClient
                        .sql("SELECT * FROM dlt_deck_audit_ledger WHERE event_id = :eventId")
                        .param("eventId", eventId)
                        .query(JdbcAuditLedgerRepository::mapEntry)
                        .single();
            });
            if (saved == null) {
                throw new IllegalStateException("Failed to append audit entry");
            }
            return saved;
        } finally {
            if (dialect == DltDeckDatabaseDialect.H2) {
                H2_SERIALIZE_LOCK.unlock();
            }
        }
    }

    @Override
    public List<AuditEntry> listRecent(int limit) {
        return jdbcClient
                .sql("SELECT * FROM dlt_deck_audit_ledger ORDER BY sequence_no DESC LIMIT :limit")
                .param("limit", Math.clamp(limit, 1, 500))
                .query(JdbcAuditLedgerRepository::mapEntry)
                .list();
    }

    @Override
    public List<AuditEntry> findAllOrdered() {
        return jdbcClient
                .sql("SELECT * FROM dlt_deck_audit_ledger ORDER BY sequence_no ASC")
                .query(JdbcAuditLedgerRepository::mapEntry)
                .list();
    }

    @Override
    public AuditChainVerification verifyIntegrity() {
        return hashChain.verifyChain(findAllOrdered());
    }

    private static AuditEntry mapEntry(ResultSet rs, int rowNum) throws SQLException {
        return new AuditEntry(
                rs.getLong("sequence_no"),
                JdbcDltMessageRepository.readUuid(rs, "event_id"),
                rs.getString("actor"),
                rs.getString("actor_role"),
                rs.getString("action"),
                rs.getString("target_type"),
                rs.getString("target_id"),
                rs.getString("merkle_root"),
                rs.getString("details_json"),
                rs.getString("prev_hash"),
                rs.getString("entry_hash"),
                JdbcDltMessageRepository.readInstant(rs, "created_at"));
    }
}
