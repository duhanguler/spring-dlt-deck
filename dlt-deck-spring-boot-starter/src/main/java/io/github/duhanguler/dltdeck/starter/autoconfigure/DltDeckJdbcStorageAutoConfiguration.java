package io.github.duhanguler.dltdeck.starter.autoconfigure;

import io.github.duhanguler.dltdeck.core.policy.AuditHashChain;
import io.github.duhanguler.dltdeck.core.policy.KeysetCursorCodec;
import io.github.duhanguler.dltdeck.core.spi.AuditLedgerRepository;
import io.github.duhanguler.dltdeck.core.spi.DltMessageRepository;
import io.github.duhanguler.dltdeck.core.spi.PayloadBlobStore;
import io.github.duhanguler.dltdeck.core.spi.ReplayPlanRepository;
import io.github.duhanguler.dltdeck.core.spi.StripeLeaseStore;
import io.github.duhanguler.dltdeck.starter.config.DltDeckProperties;
import io.github.duhanguler.dltdeck.storage.jdbc.codec.PayloadStorageCodec;
import io.github.duhanguler.dltdeck.storage.jdbc.migration.DltDeckDatabaseDialect;
import io.github.duhanguler.dltdeck.storage.jdbc.migration.DltDeckSchemaMigrator;
import io.github.duhanguler.dltdeck.storage.jdbc.repository.JdbcAuditLedgerRepository;
import io.github.duhanguler.dltdeck.storage.jdbc.repository.JdbcDltMessageRepository;
import io.github.duhanguler.dltdeck.storage.jdbc.repository.JdbcReplayPlanRepository;
import io.github.duhanguler.dltdeck.storage.jdbc.repository.JdbcStripeLeaseStore;
import java.time.Clock;
import javax.sql.DataSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.json.JsonMapper;

@AutoConfiguration(
        after = DltDeckCoreAutoConfiguration.class,
        afterName = {
            "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
            "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
            "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration",
            "org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration",
            "org.springframework.boot.liquibase.autoconfigure.LiquibaseAutoConfiguration",
            "org.springframework.boot.autoconfigure.liquibase.LiquibaseAutoConfiguration"
        })
@ConditionalOnProperty(prefix = "dlt.deck", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnBean(DataSource.class)
public class DltDeckJdbcStorageAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public DltDeckDatabaseDialect dltDeckDatabaseDialect(DataSource dataSource) {
        return DltDeckDatabaseDialect.detect(dataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    public DltDeckSchemaMigrator dltDeckSchemaMigrator(
            DataSource dataSource,
            DltDeckDatabaseDialect dialect,
            @Qualifier("dltDeckInternal") Clock clock,
            DltDeckProperties properties) {
        DltDeckSchemaMigrator migrator = new DltDeckSchemaMigrator(dataSource, dialect, clock);
        if (properties.getStorage().isAutoMigrate()) {
            migrator.migrate();
        }
        return migrator;
    }

    @Bean
    @ConditionalOnMissingBean
    public PayloadStorageCodec dltDeckPayloadStorageCodec(
            DltDeckProperties properties, ObjectProvider<PayloadBlobStore> blobStoreProvider) {
        var s = properties.getStorage();
        return new PayloadStorageCodec(
                s.getMaxPayloadBytes(),
                s.getCompressionThresholdBytes(),
                s.isGzipCompression(),
                blobStoreProvider.getIfAvailable());
    }

    @Bean
    @ConditionalOnMissingBean(DltMessageRepository.class)
    public DltMessageRepository dltDeckMessageRepository(
            DataSource dataSource,
            DltDeckDatabaseDialect dialect,
            DltDeckSchemaMigrator migrator,
            PayloadStorageCodec storageCodec,
            KeysetCursorCodec cursorCodec,
            @Qualifier("dltDeckInternal") JsonMapper jsonMapper) {
        // migrator parameter enforces startup schema creation before repository usage
        migrator.dialect();
        return new JdbcDltMessageRepository(dataSource, dialect, storageCodec, cursorCodec, jsonMapper);
    }

    @Bean
    @ConditionalOnMissingBean(ReplayPlanRepository.class)
    public ReplayPlanRepository dltDeckReplayPlanRepository(
            DataSource dataSource, DltDeckSchemaMigrator migrator) {
        migrator.dialect();
        return new JdbcReplayPlanRepository(dataSource);
    }

    @Bean
    @ConditionalOnMissingBean(StripeLeaseStore.class)
    public StripeLeaseStore dltDeckStripeLeaseStore(
            DataSource dataSource, DltDeckDatabaseDialect dialect, DltDeckSchemaMigrator migrator) {
        migrator.dialect();
        return new JdbcStripeLeaseStore(dataSource, dialect);
    }

    @Bean
    @ConditionalOnMissingBean(AuditLedgerRepository.class)
    public AuditLedgerRepository dltDeckAuditLedgerRepository(
            DataSource dataSource,
            DltDeckDatabaseDialect dialect,
            AuditHashChain auditHashChain,
            DltDeckSchemaMigrator migrator) {
        migrator.dialect();
        return new JdbcAuditLedgerRepository(dataSource, dialect, auditHashChain);
    }
}
