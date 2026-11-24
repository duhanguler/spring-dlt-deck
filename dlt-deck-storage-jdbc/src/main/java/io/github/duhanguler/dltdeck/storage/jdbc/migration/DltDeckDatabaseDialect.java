package io.github.duhanguler.dltdeck.storage.jdbc.migration;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Locale;
import javax.sql.DataSource;

public enum DltDeckDatabaseDialect {
    POSTGRESQL("postgresql"),
    H2("h2");

    private final String resourceFolder;

    DltDeckDatabaseDialect(String resourceFolder) {
        this.resourceFolder = resourceFolder;
    }

    public String resourceFolder() {
        return resourceFolder;
    }

    public static DltDeckDatabaseDialect detect(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            String product = meta.getDatabaseProductName().toLowerCase(Locale.ROOT);
            if (product.contains("postgresql")) {
                return POSTGRESQL;
            }
            if (product.contains("h2")) {
                return H2;
            }
            throw new IllegalStateException("Unsupported database dialect for spring-dlt-deck: " + product);
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to detect database dialect", ex);
        }
    }
}
