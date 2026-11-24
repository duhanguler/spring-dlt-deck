package io.github.duhanguler.dltdeck.storage.jdbc.migration;

import io.github.duhanguler.dltdeck.core.fingerprint.ExceptionFingerprinter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Isolated startup schema migrator for {@code spring-dlt-deck}.
 *
 * <p>Never touches the host application's {@code flyway_schema_history} or Liquibase tables.
 * On PostgreSQL, coordinates concurrent multi-pod startup with {@code pg_advisory_lock(hashtext('dlt_deck_schema_migration'))}
 * and tracks checksummed versions in {@code dlt_deck_schema_history}.
 */
public final class DltDeckSchemaMigrator {

    private static final Logger log = LoggerFactory.getLogger(DltDeckSchemaMigrator.class);
    private static final ReentrantLock IN_JVM_LOCK = new ReentrantLock();
    private static final String ADVISORY_LOCK_KEY = "dlt_deck_schema_migration";

    private static final List<MigrationSpec> MIGRATIONS = List.of(
            new MigrationSpec("V001", "initial_schema", "V001__initial_schema.sql"));

    private final DataSource dataSource;
    private final DltDeckDatabaseDialect dialect;
    private final Clock clock;

    public DltDeckSchemaMigrator(DataSource dataSource, Clock clock) {
        this(dataSource, DltDeckDatabaseDialect.detect(dataSource), clock);
    }

    public DltDeckSchemaMigrator(DataSource dataSource, DltDeckDatabaseDialect dialect, Clock clock) {
        this.dataSource = dataSource;
        this.dialect = dialect;
        this.clock = clock;
    }

    public DltDeckDatabaseDialect dialect() {
        return dialect;
    }

    public void migrate() {
        IN_JVM_LOCK.lock();
        try (Connection conn = dataSource.getConnection()) {
            boolean originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(true);
            acquireAdvisoryLock(conn);
            try {
                ensureHistoryTable(conn);
                for (MigrationSpec spec : MIGRATIONS) {
                    applyIfNeeded(conn, spec);
                }
            } finally {
                releaseAdvisoryLock(conn);
                conn.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("spring-dlt-deck schema migration failed", ex);
        } finally {
            IN_JVM_LOCK.unlock();
        }
    }

    private void acquireAdvisoryLock(Connection conn) throws SQLException {
        if (dialect == DltDeckDatabaseDialect.POSTGRESQL) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT pg_advisory_lock(hashtext(?))")) {
                ps.setString(1, ADVISORY_LOCK_KEY);
                ps.execute();
            }
        }
    }

    private void releaseAdvisoryLock(Connection conn) {
        if (dialect == DltDeckDatabaseDialect.POSTGRESQL) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT pg_advisory_unlock(hashtext(?))")) {
                ps.setString(1, ADVISORY_LOCK_KEY);
                ps.execute();
            } catch (SQLException ex) {
                log.warn("Failed to release PostgreSQL advisory lock for dlt_deck_schema_migration", ex);
            }
        }
    }

    private void ensureHistoryTable(Connection conn) throws SQLException {
        String tsType = dialect == DltDeckDatabaseDialect.POSTGRESQL ? "TIMESTAMPTZ" : "TIMESTAMP WITH TIME ZONE";
        String sql = """
                CREATE TABLE IF NOT EXISTS dlt_deck_schema_history (
                    version     VARCHAR(64) PRIMARY KEY,
                    description VARCHAR(255) NOT NULL,
                    checksum    VARCHAR(64) NOT NULL,
                    applied_at  %s NOT NULL
                )
                """.formatted(tsType);
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private void applyIfNeeded(Connection conn, MigrationSpec spec) throws SQLException {
        String sql = loadSqlResource(spec.filename());
        String checksum = ExceptionFingerprinter.sha256Hex(sql);

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT checksum FROM dlt_deck_schema_history WHERE version = ?")) {
            ps.setString(1, spec.version());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String existing = rs.getString(1);
                    if (!checksum.equals(existing)) {
                        throw new IllegalStateException("Checksum mismatch for dlt-deck migration "
                                + spec.version() + ": expected " + existing + " but found " + checksum);
                    }
                    return;
                }
            }
        }

        conn.setAutoCommit(false);
        try {
            for (String statement : splitSqlStatements(sql)) {
                try (Statement st = conn.createStatement()) {
                    st.execute(statement);
                }
            }
            try (PreparedStatement insert = conn.prepareStatement(
                    "INSERT INTO dlt_deck_schema_history (version, description, checksum, applied_at) VALUES (?, ?, ?, ?)")) {
                insert.setString(1, spec.version());
                insert.setString(2, spec.description());
                insert.setString(3, checksum);
                insert.setTimestamp(4, Timestamp.from(Instant.now(clock)));
                insert.executeUpdate();
            }
            conn.commit();
            log.info("Applied spring-dlt-deck schema migration {} ({}) on {}", spec.version(), spec.description(), dialect);
        } catch (SQLException ex) {
            conn.rollback();
            throw ex;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    private String loadSqlResource(String filename) {
        String path = "dltdeck/sql/" + dialect.resourceFolder() + "/" + filename;
        try (InputStream in = DltDeckSchemaMigrator.class.getClassLoader().getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("Missing SQL migration resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to read SQL migration resource: " + path, ex);
        }
    }

    private static List<String> splitSqlStatements(String script) {
        StringBuilder cleaned = new StringBuilder(script.length());
        for (String line : script.lines().toList()) {
            String trimmed = line.trim();
            if (!trimmed.startsWith("--")) {
                cleaned.append(line).append('\n');
            }
        }
        List<String> statements = new ArrayList<>();
        int start = 0;
        String text = cleaned.toString();
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == ';') {
                String stmt = text.substring(start, i).trim();
                if (!stmt.isEmpty()) {
                    statements.add(stmt);
                }
                start = i + 1;
            }
        }
        String tail = text.substring(start).trim();
        if (!tail.isEmpty()) {
            statements.add(tail);
        }
        return statements;
    }

    private record MigrationSpec(String version, String description, String filename) {}
}
