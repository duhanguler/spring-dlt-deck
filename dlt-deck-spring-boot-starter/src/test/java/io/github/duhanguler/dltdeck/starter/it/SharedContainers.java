package io.github.duhanguler.dltdeck.starter.it;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Shared Testcontainers 2.0.5 infrastructure (PostgreSQL 17 + Apache Kafka 4 KRaft)
 * reused across all fault-injection ITs for deterministic, fast execution.
 */
public final class SharedContainers {

    public static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine")
            .withStartupTimeout(Duration.ofMinutes(2))
            .withStartupAttempts(2)
            .withReuse(false);

    public static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:4.0.0")
            .withStartupTimeout(Duration.ofMinutes(2))
            .withStartupAttempts(2)
            .withReuse(false);

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    private SharedContainers() {}

    public static void createTopics(Map<String, Integer> topicPartitions) throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            var existing = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            List<NewTopic> toCreate = topicPartitions.entrySet().stream()
                    .filter(e -> !existing.contains(e.getKey()))
                    .map(e -> new NewTopic(e.getKey(), e.getValue(), (short) 1))
                    .toList();
            if (!toCreate.isEmpty()) {
                admin.createTopics(toCreate).all().get(15, TimeUnit.SECONDS);
            }
        }
    }

    public static void truncateDltDeckTables(JdbcTemplate jdbc) {
        jdbc.execute("""
                TRUNCATE TABLE dlt_deck_plan_stripes,
                               dlt_deck_replay_plans,
                               dlt_deck_payloads,
                               dlt_deck_messages,
                               dlt_deck_fingerprints,
                               dlt_deck_audit_ledger
                RESTART IDENTITY CASCADE
                """);
    }
}
