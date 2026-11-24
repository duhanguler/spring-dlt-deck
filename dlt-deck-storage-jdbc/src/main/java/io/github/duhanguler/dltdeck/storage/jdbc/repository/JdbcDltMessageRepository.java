package io.github.duhanguler.dltdeck.storage.jdbc.repository;

import io.github.duhanguler.dltdeck.core.codec.DefaultPayloadCodec;
import io.github.duhanguler.dltdeck.core.codec.PayloadCodec;
import io.github.duhanguler.dltdeck.core.model.DltMessageDetail;
import io.github.duhanguler.dltdeck.core.model.DltMessageSummary;
import io.github.duhanguler.dltdeck.core.model.FailureCategory;
import io.github.duhanguler.dltdeck.core.model.FingerprintCluster;
import io.github.duhanguler.dltdeck.core.model.IngestedDltMessage;
import io.github.duhanguler.dltdeck.core.model.KeysetPage;
import io.github.duhanguler.dltdeck.core.model.MessageStatus;
import io.github.duhanguler.dltdeck.core.model.PayloadFormat;
import io.github.duhanguler.dltdeck.core.policy.AuditHashChain;
import io.github.duhanguler.dltdeck.core.policy.KeysetCursorCodec;
import io.github.duhanguler.dltdeck.core.spi.DltMessageRepository;
import io.github.duhanguler.dltdeck.storage.jdbc.codec.PayloadStorageCodec;
import io.github.duhanguler.dltdeck.storage.jdbc.migration.DltDeckDatabaseDialect;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * Spring {@link JdbcClient} + JDBC {@code batchUpdate} implementation of the 3-table Hot/Cold/Deduplicated store.
 *
 * <p>Uses short, bounded transactions exclusively:
 * <ul>
 *   <li>TX1 ({@link #claimForReplay}): claims a single record and increments {@code fencing_token}, committing immediately before Kafka I/O.</li>
 *   <li>TX2 ({@link #markPublished} / {@link #markFailedPublish}): commits post-Kafka outcome fenced by {@code fencing_token}.</li>
 * </ul>
 */
public final class JdbcDltMessageRepository implements DltMessageRepository {

    private static final TypeReference<Map<String, String>> HEADER_MAP_TYPE = new TypeReference<>() {};

    private final JdbcClient jdbcClient;
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate txTemplate;
    private final DltDeckDatabaseDialect dialect;
    private final PayloadStorageCodec payloadStorageCodec;
    private final KeysetCursorCodec cursorCodec;
    private final PayloadCodec payloadCodec;
    private final JsonMapper jsonMapper;

    public JdbcDltMessageRepository(
            DataSource dataSource,
            DltDeckDatabaseDialect dialect,
            PayloadStorageCodec payloadStorageCodec,
            KeysetCursorCodec cursorCodec,
            JsonMapper jsonMapper) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.jdbcClient = JdbcClient.create(this.jdbcTemplate);
        this.txTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        this.dialect = dialect;
        this.payloadStorageCodec = payloadStorageCodec;
        this.cursorCodec = cursorCodec;
        this.payloadCodec = new DefaultPayloadCodec(jsonMapper);
        this.jsonMapper = jsonMapper;
    }

    @Override
    public int ingestBatch(List<IngestedDltMessage> messages) {
        if (messages.isEmpty()) {
            return 0;
        }
        Integer inserted = txTemplate.execute(_ -> {
            upsertFingerprints(messages);
            List<IngestedDltMessage> insertedMessages = insertHotMessages(messages);
            if (!insertedMessages.isEmpty()) {
                insertColdPayloads(insertedMessages);
            }
            return insertedMessages.size();
        });
        return inserted == null ? 0 : inserted;
    }

    private void upsertFingerprints(List<IngestedDltMessage> messages) {
        Map<String, IngestedDltMessage> uniqueByFp = new LinkedHashMap<>();
        for (IngestedDltMessage msg : messages) {
            uniqueByFp.put(msg.fingerprint(), msg);
        }
        List<IngestedDltMessage> uniqueList = List.copyOf(uniqueByFp.values());
        String sql = dialect == DltDeckDatabaseDialect.POSTGRESQL
                ? """
                  INSERT INTO dlt_deck_fingerprints (
                      fingerprint, exception_fqcn, root_cause_fqcn, sample_message,
                      canonical_stacktrace, failure_category, recommendation, first_seen_at, last_seen_at
                  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                  ON CONFLICT (fingerprint) DO UPDATE SET
                      last_seen_at = GREATEST(dlt_deck_fingerprints.last_seen_at, EXCLUDED.last_seen_at)
                  """
                : """
                  MERGE INTO dlt_deck_fingerprints (
                      fingerprint, exception_fqcn, root_cause_fqcn, sample_message,
                      canonical_stacktrace, failure_category, recommendation, first_seen_at, last_seen_at
                  ) KEY (fingerprint) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                  """;

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                IngestedDltMessage m = uniqueList.get(i);
                Timestamp ts = Timestamp.from(AuditHashChain.normalizeTimestamp(m.ingestedAt()));
                ps.setString(1, m.fingerprint());
                ps.setString(2, m.exceptionFqcn());
                ps.setString(3, m.rootCauseFqcn());
                ps.setString(4, m.sampleMessage());
                ps.setString(5, m.canonicalStacktrace());
                ps.setString(6, m.failureCategory().name());
                ps.setString(7, m.recommendation());
                ps.setTimestamp(8, ts);
                ps.setTimestamp(9, ts);
            }

            @Override
            public int getBatchSize() {
                return uniqueList.size();
            }
        });
    }

    private List<IngestedDltMessage> insertHotMessages(List<IngestedDltMessage> messages) {
        if (dialect == DltDeckDatabaseDialect.POSTGRESQL) {
            String sql = """
                    INSERT INTO dlt_deck_messages (
                        id, dlt_topic, dlt_partition, dlt_offset,
                        original_topic, original_partition, original_offset, original_consumer_group,
                        message_key_hash, key_fingerprint, payload_format, schema_id, payload_size_bytes,
                        fingerprint, failure_category, replay_hops, status, active_plan_id,
                        fencing_token, ingested_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                    ON CONFLICT (dlt_topic, dlt_partition, dlt_offset) DO NOTHING
                    """;
            int[] counts = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    bindHotMessage(ps, messages.get(i));
                }

                @Override
                public int getBatchSize() {
                    return messages.size();
                }
            });
            List<IngestedDltMessage> actuallyInserted = new ArrayList<>();
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] > 0) {
                    actuallyInserted.add(messages.get(i));
                }
            }
            return actuallyInserted;
        }

        // H2 path: idempotent check + insert per message within the same short transaction
        String existsSql =
                "SELECT COUNT(*) FROM dlt_deck_messages WHERE dlt_topic = ? AND dlt_partition = ? AND dlt_offset = ?";
        String insertSql = """
                INSERT INTO dlt_deck_messages (
                    id, dlt_topic, dlt_partition, dlt_offset,
                    original_topic, original_partition, original_offset, original_consumer_group,
                    message_key_hash, key_fingerprint, payload_format, schema_id, payload_size_bytes,
                    fingerprint, failure_category, replay_hops, status, active_plan_id,
                    fencing_token, ingested_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                """;
        List<IngestedDltMessage> inserted = new ArrayList<>();
        for (IngestedDltMessage m : messages) {
            Integer existing = jdbcTemplate.queryForObject(
                    existsSql, Integer.class, m.dltTopic(), m.dltPartition(), m.dltOffset());
            if (existing == null || existing == 0) {
                jdbcTemplate.update(insertSql, ps -> bindHotMessage(ps, m));
                inserted.add(m);
            }
        }
        return inserted;
    }

    private static void bindHotMessage(PreparedStatement ps, IngestedDltMessage m) throws SQLException {
        Timestamp ts = Timestamp.from(AuditHashChain.normalizeTimestamp(m.ingestedAt()));
        ps.setObject(1, m.id());
        ps.setString(2, m.dltTopic());
        ps.setInt(3, m.dltPartition());
        ps.setLong(4, m.dltOffset());
        ps.setString(5, m.originalTopic());
        if (m.originalPartition() != null) {
            ps.setInt(6, m.originalPartition());
        } else {
            ps.setNull(6, Types.INTEGER);
        }
        if (m.originalOffset() != null) {
            ps.setLong(7, m.originalOffset());
        } else {
            ps.setNull(7, Types.BIGINT);
        }
        ps.setString(8, m.originalConsumerGroup());
        ps.setInt(9, m.messageKeyHash());
        ps.setString(10, m.keyFingerprint());
        ps.setString(11, m.payloadFormat().name());
        if (m.schemaId() != null) {
            ps.setInt(12, m.schemaId());
        } else {
            ps.setNull(12, Types.INTEGER);
        }
        ps.setInt(13, m.payloadSizeBytes());
        ps.setString(14, m.fingerprint());
        ps.setString(15, m.failureCategory().name());
        ps.setInt(16, m.replayHops());
        ps.setString(17, m.initialStatus().name());
        if (m.correlatedPlanId() != null) {
            ps.setObject(18, m.correlatedPlanId());
        } else {
            ps.setNull(18, Types.OTHER);
        }
        ps.setTimestamp(19, ts);
        ps.setTimestamp(20, ts);
    }

    private void insertColdPayloads(List<IngestedDltMessage> messages) {
        String sql = """
                INSERT INTO dlt_deck_payloads (
                    message_id, message_key, payload, payload_encoding, external_blob_uri, headers_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """;
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                IngestedDltMessage m = messages.get(i);
                PayloadStorageCodec.EncodedPayload encoded = payloadStorageCodec.encode(m.id(), m.payload());
                ps.setObject(1, m.id());
                byte[] keyBytes = m.messageKey();
                if (keyBytes != null) {
                    ps.setBytes(2, keyBytes);
                } else {
                    ps.setNull(2, Types.BINARY);
                }
                if (encoded.bytes() != null) {
                    ps.setBytes(3, encoded.bytes());
                } else {
                    ps.setNull(3, Types.BINARY);
                }
                ps.setString(4, encoded.encoding());
                ps.setString(5, encoded.externalBlobUri());
                ps.setString(6, serializeHeaders(m.headers()));
            }

            @Override
            public int getBatchSize() {
                return messages.size();
            }
        });
    }

    @Override
    public List<FingerprintCluster> listClusters(@Nullable String dltTopic, int limit) {
        int safeLimit = Math.clamp(limit, 1, 200);
        StringBuilder sql = new StringBuilder("""
                SELECT f.fingerprint, f.exception_fqcn, f.root_cause_fqcn, f.sample_message,
                       f.canonical_stacktrace, f.failure_category, f.recommendation,
                       f.first_seen_at, f.last_seen_at,
                       COUNT(m.id) AS total_count,
                       SUM(CASE WHEN m.status IN ('NEW', 'FAILED_PUBLISH') THEN 1 ELSE 0 END) AS new_count
                FROM dlt_deck_fingerprints f
                JOIN dlt_deck_messages m ON m.fingerprint = f.fingerprint
                """);
        Map<String, Object> params = new LinkedHashMap<>();
        if (dltTopic != null && !dltTopic.isBlank()) {
            sql.append(" WHERE m.dlt_topic = :dltTopic ");
            params.put("dltTopic", dltTopic);
        }
        sql.append("""
                GROUP BY f.fingerprint, f.exception_fqcn, f.root_cause_fqcn, f.sample_message,
                         f.canonical_stacktrace, f.failure_category, f.recommendation,
                         f.first_seen_at, f.last_seen_at
                ORDER BY total_count DESC, f.last_seen_at DESC
                LIMIT :limit
                """);
        params.put("limit", safeLimit);

        JdbcClient.StatementSpec spec = jdbcClient.sql(sql.toString());
        for (Map.Entry<String, Object> e : params.entrySet()) {
            spec = spec.param(e.getKey(), e.getValue());
        }
        return spec.query(JdbcDltMessageRepository::mapCluster).list();
    }

    @Override
    public KeysetPage<DltMessageSummary> listMessages(
            @Nullable String dltTopic,
            @Nullable String fingerprint,
            @Nullable MessageStatus status,
            @Nullable String cursor,
            int limit) {
        int safeLimit = Math.clamp(limit, 1, 200);
        StringBuilder sql = new StringBuilder("SELECT * FROM dlt_deck_messages WHERE 1=1");
        Map<String, Object> params = new LinkedHashMap<>();

        if (dltTopic != null && !dltTopic.isBlank()) {
            sql.append(" AND dlt_topic = :dltTopic");
            params.put("dltTopic", dltTopic);
        }
        if (fingerprint != null && !fingerprint.isBlank()) {
            sql.append(" AND fingerprint = :fingerprint");
            params.put("fingerprint", fingerprint);
        }
        if (status != null) {
            sql.append(" AND status = :status");
            params.put("status", status.name());
        }
        if (cursor != null && !cursor.isBlank()) {
            KeysetCursorCodec.CursorToken token = cursorCodec.decode(cursor);
            sql.append(" AND (ingested_at < :cursorTs OR (ingested_at = :cursorTs AND id < :cursorId))");
            params.put("cursorTs", Timestamp.from(token.ingestedAt()));
            params.put("cursorId", token.id());
        }
        sql.append(" ORDER BY ingested_at DESC, id DESC LIMIT :limit");
        params.put("limit", safeLimit + 1);

        JdbcClient.StatementSpec spec = jdbcClient.sql(sql.toString());
        for (Map.Entry<String, Object> e : params.entrySet()) {
            spec = spec.param(e.getKey(), e.getValue());
        }
        List<DltMessageSummary> fetched = spec.query(JdbcDltMessageRepository::mapSummary).list();
        boolean hasMore = fetched.size() > safeLimit;
        List<DltMessageSummary> pageItems = hasMore ? fetched.subList(0, safeLimit) : fetched;
        String nextCursor = null;
        if (hasMore && !pageItems.isEmpty()) {
            DltMessageSummary last = pageItems.getLast();
            nextCursor = cursorCodec.encode(last.ingestedAt(), last.id());
        }
        return new KeysetPage<>(pageItems, nextCursor, hasMore);
    }

    @Override
    public Optional<DltMessageDetail> findDetailById(UUID messageId) {
        String sql = """
                SELECT m.*,
                       f.exception_fqcn, f.root_cause_fqcn, f.sample_message, f.canonical_stacktrace,
                       f.recommendation, f.first_seen_at, f.last_seen_at,
                       p.message_key, p.payload, p.payload_encoding, p.external_blob_uri, p.headers_json
                FROM dlt_deck_messages m
                JOIN dlt_deck_fingerprints f ON f.fingerprint = m.fingerprint
                JOIN dlt_deck_payloads p ON p.message_id = m.id
                WHERE m.id = :id
                """;
        return jdbcClient
                .sql(sql)
                .param("id", messageId)
                .query((rs, _) -> {
                    DltMessageSummary summary = mapSummary(rs, 0);
                    FingerprintCluster cluster = new FingerprintCluster(
                            summary.fingerprint(),
                            rs.getString("exception_fqcn"),
                            rs.getString("root_cause_fqcn"),
                            rs.getString("sample_message"),
                            rs.getString("canonical_stacktrace"),
                            summary.failureCategory(),
                            rs.getString("recommendation"),
                            readInstant(rs, "first_seen_at"),
                            readInstant(rs, "last_seen_at"),
                            1L,
                            summary.status().isReplayEligible() ? 1L : 0L);
                    byte[] keyBytes = rs.getBytes("message_key");
                    String encoding = rs.getString("payload_encoding");
                    String blobUri = rs.getString("external_blob_uri");
                    byte[] payloadBytes = payloadStorageCodec.decode(rs.getBytes("payload"), encoding, blobUri);
                    Map<String, String> headers = deserializeHeaders(rs.getString("headers_json"));
                    PayloadCodec.DecodedPayload decoded = payloadCodec.decode(payloadBytes, headers, false);
                    return new DltMessageDetail(
                            summary,
                            cluster,
                            keyBytes,
                            payloadBytes,
                            encoding,
                            blobUri,
                            headers,
                            decoded.cloudEventType(),
                            decoded.cloudEventId(),
                            decoded.cloudEventSource());
                })
                .optional();
    }

    @Override
    public int countEligibleForPlan(
            @Nullable String dltTopicFilter, @Nullable String fingerprintFilter, int maxRecords) {
        StringBuilder sql = new StringBuilder(
                "SELECT COUNT(*) FROM dlt_deck_messages WHERE status IN ('NEW', 'FAILED_PUBLISH')");
        Map<String, Object> params = new LinkedHashMap<>();
        if (dltTopicFilter != null && !dltTopicFilter.isBlank()) {
            sql.append(" AND dlt_topic = :dltTopic");
            params.put("dltTopic", dltTopicFilter);
        }
        if (fingerprintFilter != null && !fingerprintFilter.isBlank()) {
            sql.append(" AND fingerprint = :fingerprint");
            params.put("fingerprint", fingerprintFilter);
        }
        JdbcClient.StatementSpec spec = jdbcClient.sql(sql.toString());
        for (Map.Entry<String, Object> e : params.entrySet()) {
            spec = spec.param(e.getKey(), e.getValue());
        }
        Long total = spec.query(Long.class).single();
        return (int) Math.min(total, maxRecords);
    }

    @Override
    public List<DltMessageSummary> findEligibleForStripe(
            @Nullable String dltTopicFilter,
            @Nullable String fingerprintFilter,
            int stripeCount,
            int stripeId,
            int limit) {
        int safeLimit = Math.clamp(limit, 1, 500);
        StringBuilder sql = new StringBuilder("""
                SELECT * FROM dlt_deck_messages
                WHERE status IN ('NEW', 'FAILED_PUBLISH')
                  AND (MOD(MOD(COALESCE(original_partition, message_key_hash), :stripeCount) + :stripeCount, :stripeCount)) = :stripeId
                """);
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("stripeCount", stripeCount);
        params.put("stripeId", stripeId);
        if (dltTopicFilter != null && !dltTopicFilter.isBlank()) {
            sql.append(" AND dlt_topic = :dltTopic");
            params.put("dltTopic", dltTopicFilter);
        }
        if (fingerprintFilter != null && !fingerprintFilter.isBlank()) {
            sql.append(" AND fingerprint = :fingerprint");
            params.put("fingerprint", fingerprintFilter);
        }
        sql.append(" ORDER BY COALESCE(original_offset, dlt_offset) ASC, dlt_offset ASC LIMIT :limit");
        params.put("limit", safeLimit);

        JdbcClient.StatementSpec spec = jdbcClient.sql(sql.toString());
        for (Map.Entry<String, Object> e : params.entrySet()) {
            spec = spec.param(e.getKey(), e.getValue());
        }
        return spec.query(JdbcDltMessageRepository::mapSummary).list();
    }

    @Override
    public Optional<ClaimedMessage> claimForReplay(
            UUID messageId, UUID planId, String leaseOwner, Instant leaseExpiresAt, Instant now) {
        return txTemplate.execute(_ -> {
            int updated = jdbcClient
                    .sql("""
                            UPDATE dlt_deck_messages
                            SET status = 'CLAIMED',
                                active_plan_id = :planId,
                                fencing_token = fencing_token + 1,
                                lease_owner = :owner,
                                lease_expires_at = :expiresAt,
                                updated_at = :now
                            WHERE id = :id AND status IN ('NEW', 'FAILED_PUBLISH')
                            """)
                    .param("planId", planId)
                    .param("owner", leaseOwner)
                    .param("expiresAt", Timestamp.from(AuditHashChain.normalizeTimestamp(leaseExpiresAt)))
                    .param("now", Timestamp.from(AuditHashChain.normalizeTimestamp(now)))
                    .param("id", messageId)
                    .update();
            if (updated == 0) {
                return Optional.empty();
            }
            return findDetailById(messageId)
                    .map(detail -> new ClaimedMessage(detail, detail.summary().fencingToken()));
        });
    }

    @Override
    public boolean markPublished(
            UUID messageId,
            long expectedFencingToken,
            int publishedPartition,
            long publishedOffset,
            Instant now) {
        Boolean ok = txTemplate.execute(_ -> jdbcClient
                        .sql("""
                                UPDATE dlt_deck_messages
                                SET status = 'PUBLISHED',
                                    published_partition = :pubPartition,
                                    published_offset = :pubOffset,
                                    lease_owner = NULL,
                                    lease_expires_at = NULL,
                                    updated_at = :now
                                WHERE id = :id AND status = 'CLAIMED' AND fencing_token = :token
                                """)
                        .param("pubPartition", publishedPartition)
                        .param("pubOffset", publishedOffset)
                        .param("now", Timestamp.from(AuditHashChain.normalizeTimestamp(now)))
                        .param("id", messageId)
                        .param("token", expectedFencingToken)
                        .update()
                > 0);
        return Boolean.TRUE.equals(ok);
    }

    @Override
    public boolean markFailedPublish(UUID messageId, long expectedFencingToken, Instant now) {
        Boolean ok = txTemplate.execute(_ -> jdbcClient
                        .sql("""
                                UPDATE dlt_deck_messages
                                SET status = 'FAILED_PUBLISH',
                                    lease_owner = NULL,
                                    lease_expires_at = NULL,
                                    updated_at = :now
                                WHERE id = :id AND status = 'CLAIMED' AND fencing_token = :token
                                """)
                        .param("now", Timestamp.from(AuditHashChain.normalizeTimestamp(now)))
                        .param("id", messageId)
                        .param("token", expectedFencingToken)
                        .update()
                > 0);
        return Boolean.TRUE.equals(ok);
    }

    @Override
    public int blockSubsequentForKey(
            UUID planId, String dltTopic, String keyFingerprint, @Nullable UUID excludeMessageId, Instant now) {
        Integer count = txTemplate.execute(_ -> {
            if (excludeMessageId != null) {
                return jdbcClient
                        .sql("""
                                UPDATE dlt_deck_messages
                                SET status = 'BLOCKED_BY_PRIOR_KEY_FAILURE',
                                    active_plan_id = :planId,
                                    updated_at = :now
                                WHERE dlt_topic = :dltTopic
                                  AND key_fingerprint = :keyFp
                                  AND id <> :excludeId
                                  AND status = 'NEW'
                                """)
                        .param("planId", planId)
                        .param("now", Timestamp.from(AuditHashChain.normalizeTimestamp(now)))
                        .param("dltTopic", dltTopic)
                        .param("keyFp", keyFingerprint)
                        .param("excludeId", excludeMessageId)
                        .update();
            }
            return jdbcClient
                    .sql("""
                            UPDATE dlt_deck_messages
                            SET status = 'BLOCKED_BY_PRIOR_KEY_FAILURE',
                                active_plan_id = :planId,
                                updated_at = :now
                            WHERE dlt_topic = :dltTopic
                              AND key_fingerprint = :keyFp
                              AND status = 'NEW'
                            """)
                    .param("planId", planId)
                    .param("now", Timestamp.from(AuditHashChain.normalizeTimestamp(now)))
                    .param("dltTopic", dltTopic)
                    .param("keyFp", keyFingerprint)
                    .update();
        });
        return count == null ? 0 : count;
    }

    @Override
    public List<DltMessageSummary> sweepExpiredLeasesToIndeterminate(Instant now, int limit) {
        Timestamp nowTs = Timestamp.from(AuditHashChain.normalizeTimestamp(now));
        List<DltMessageSummary> result = txTemplate.execute(_ -> {
            List<DltMessageSummary> expired = jdbcClient
                    .sql("""
                            SELECT * FROM dlt_deck_messages
                            WHERE status = 'CLAIMED' AND lease_expires_at < :now
                            ORDER BY lease_expires_at ASC
                            LIMIT :limit
                            """)
                    .param("now", nowTs)
                    .param("limit", Math.clamp(limit, 1, 500))
                    .query(JdbcDltMessageRepository::mapSummary)
                    .list();
            List<DltMessageSummary> transitioned = new ArrayList<>();
            for (DltMessageSummary msg : expired) {
                int updated = jdbcClient
                        .sql("""
                                UPDATE dlt_deck_messages
                                SET status = 'INDETERMINATE',
                                    updated_at = :now
                                WHERE id = :id AND status = 'CLAIMED' AND fencing_token = :token
                                """)
                        .param("now", nowTs)
                        .param("id", msg.id())
                        .param("token", msg.fencingToken())
                        .update();
                if (updated > 0) {
                    transitioned.add(msg);
                }
            }
            return transitioned;
        });
        return result == null ? List.of() : result;
    }

    @Override
    public boolean resolveIndeterminate(UUID messageId, MessageStatus targetStatus, Instant now) {
        if (targetStatus != MessageStatus.NEW
                && targetStatus != MessageStatus.PUBLISHED
                && targetStatus != MessageStatus.DISCARDED) {
            throw new IllegalArgumentException("Invalid resolution target status: " + targetStatus);
        }
        Boolean ok = txTemplate.execute(_ -> jdbcClient
                        .sql("""
                                UPDATE dlt_deck_messages
                                SET status = :targetStatus,
                                    lease_owner = NULL,
                                    lease_expires_at = NULL,
                                    updated_at = :now
                                WHERE id = :id AND status IN ('INDETERMINATE', 'BLOCKED_BY_PRIOR_KEY_FAILURE', 'QUARANTINED')
                                """)
                        .param("targetStatus", targetStatus.name())
                        .param("now", Timestamp.from(AuditHashChain.normalizeTimestamp(now)))
                        .param("id", messageId)
                        .update()
                > 0);
        return Boolean.TRUE.equals(ok);
    }

    @Override
    public boolean discardMessage(UUID messageId, Instant now) {
        Boolean ok = txTemplate.execute(_ -> jdbcClient
                        .sql("""
                                UPDATE dlt_deck_messages
                                SET status = 'DISCARDED',
                                    lease_owner = NULL,
                                    lease_expires_at = NULL,
                                    updated_at = :now
                                WHERE id = :id AND status NOT IN ('PUBLISHED', 'DISCARDED')
                                """)
                        .param("now", Timestamp.from(AuditHashChain.normalizeTimestamp(now)))
                        .param("id", messageId)
                        .update()
                > 0);
        return Boolean.TRUE.equals(ok);
    }

    @Override
    public boolean markCorrelatedReDlt(String dltTopic, int dltPartition, long dltOffset, Instant now) {
        Boolean ok = txTemplate.execute(_ -> jdbcClient
                        .sql("""
                                UPDATE dlt_deck_messages
                                SET status = 'RE_DLT_DETECTED',
                                    updated_at = :now
                                WHERE dlt_topic = :topic AND dlt_partition = :part AND dlt_offset = :off
                                  AND status = 'PUBLISHED'
                                """)
                        .param("now", Timestamp.from(AuditHashChain.normalizeTimestamp(now)))
                        .param("topic", dltTopic)
                        .param("part", dltPartition)
                        .param("off", dltOffset)
                        .update()
                > 0);
        return Boolean.TRUE.equals(ok);
    }

    @Override
    public int purgeOlderThan(Instant cutoff, int batchLimit) {
        Integer deleted = txTemplate.execute(_ -> jdbcClient
                .sql("""
                        DELETE FROM dlt_deck_messages
                        WHERE id IN (
                            SELECT id FROM dlt_deck_messages
                            WHERE ingested_at < :cutoff
                            ORDER BY ingested_at ASC
                            LIMIT :limit
                        )
                        """)
                .param("cutoff", Timestamp.from(AuditHashChain.normalizeTimestamp(cutoff)))
                .param("limit", Math.clamp(batchLimit, 1, 5000))
                .update());
        return deleted == null ? 0 : deleted;
    }

    private String serializeHeaders(Map<String, String> headers) {
        try {
            return jsonMapper.writeValueAsString(headers);
        } catch (JacksonException ex) {
            throw new IllegalStateException("Failed to serialize headers JSON", ex);
        }
    }

    private Map<String, String> deserializeHeaders(String json) {
        try {
            return jsonMapper.readValue(json, HEADER_MAP_TYPE);
        } catch (JacksonException ex) {
            return Map.of();
        }
    }

    private static DltMessageSummary mapSummary(ResultSet rs, int rowNum) throws SQLException {
        return new DltMessageSummary(
                readUuid(rs, "id"),
                rs.getString("dlt_topic"),
                rs.getInt("dlt_partition"),
                rs.getLong("dlt_offset"),
                rs.getString("original_topic"),
                readNullableInt(rs, "original_partition"),
                readNullableLong(rs, "original_offset"),
                rs.getString("original_consumer_group"),
                rs.getInt("message_key_hash"),
                rs.getString("key_fingerprint"),
                PayloadFormat.valueOf(rs.getString("payload_format")),
                readNullableInt(rs, "schema_id"),
                rs.getInt("payload_size_bytes"),
                rs.getString("fingerprint"),
                FailureCategory.valueOf(rs.getString("failure_category")),
                rs.getInt("replay_hops"),
                MessageStatus.valueOf(rs.getString("status")),
                readNullableUuid(rs, "active_plan_id"),
                rs.getLong("fencing_token"),
                rs.getString("lease_owner"),
                readNullableInstant(rs, "lease_expires_at"),
                readNullableInt(rs, "published_partition"),
                readNullableLong(rs, "published_offset"),
                readInstant(rs, "ingested_at"),
                readInstant(rs, "updated_at"));
    }

    private static FingerprintCluster mapCluster(ResultSet rs, int rowNum) throws SQLException {
        return new FingerprintCluster(
                rs.getString("fingerprint"),
                rs.getString("exception_fqcn"),
                rs.getString("root_cause_fqcn"),
                rs.getString("sample_message"),
                rs.getString("canonical_stacktrace"),
                FailureCategory.valueOf(rs.getString("failure_category")),
                rs.getString("recommendation"),
                readInstant(rs, "first_seen_at"),
                readInstant(rs, "last_seen_at"),
                rs.getLong("total_count"),
                rs.getLong("new_count"));
    }

    static UUID readUuid(ResultSet rs, String col) throws SQLException {
        Object val = rs.getObject(col);
        if (val instanceof UUID u) {
            return u;
        }
        return UUID.fromString(String.valueOf(val));
    }

    static @Nullable UUID readNullableUuid(ResultSet rs, String col) throws SQLException {
        Object val = rs.getObject(col);
        if (val == null) {
            return null;
        }
        return val instanceof UUID u ? u : UUID.fromString(String.valueOf(val));
    }

    static @Nullable Integer readNullableInt(ResultSet rs, String col) throws SQLException {
        int val = rs.getInt(col);
        return rs.wasNull() ? null : val;
    }

    static @Nullable Long readNullableLong(ResultSet rs, String col) throws SQLException {
        long val = rs.getLong(col);
        return rs.wasNull() ? null : val;
    }

    static Instant readInstant(ResultSet rs, String col) throws SQLException {
        Timestamp ts = rs.getTimestamp(col);
        return AuditHashChain.normalizeTimestamp(ts.toInstant());
    }

    static @Nullable Instant readNullableInstant(ResultSet rs, String col) throws SQLException {
        Timestamp ts = rs.getTimestamp(col);
        return ts == null ? null : AuditHashChain.normalizeTimestamp(ts.toInstant());
    }
}
