package io.github.duhanguler.dltdeck.core.policy;

import io.github.duhanguler.dltdeck.core.exception.ValidationDomainException;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Policy guard preventing operators from accidentally replaying records into Kafka internal topics
 * or back into dead-letter topics.
 */
public final class TargetTopicGuard {

    private static final Pattern VALID_KAFKA_TOPIC = Pattern.compile("^[a-zA-Z0-9._-]{1,249}$");
    private static final List<String> FORBIDDEN_DLT_SUFFIXES =
            List.of(".dlt", "-dlt", "_dlt", ".dlq", "-dlq", "_dlq");

    private TargetTopicGuard() {}

    public static void validateTargetTopicName(String topic) {
        if (topic.isBlank()) {
            throw new ValidationDomainException("INVALID_TARGET_TOPIC", "Target topic must not be blank");
        }
        String trimmed = topic.trim();
        if (".".equals(trimmed) || "..".equals(trimmed) || !VALID_KAFKA_TOPIC.matcher(trimmed).matches()) {
            throw new ValidationDomainException(
                    "INVALID_TARGET_TOPIC", "Target topic '" + trimmed + "' is not a valid Kafka topic name");
        }
        if (trimmed.startsWith("__")) {
            throw new ValidationDomainException(
                    "FORBIDDEN_INTERNAL_TOPIC", "Replaying into Kafka internal topic '" + trimmed + "' is forbidden");
        }
        String lower = trimmed.toLowerCase(Locale.ROOT);
        for (String suffix : FORBIDDEN_DLT_SUFFIXES) {
            if (lower.endsWith(suffix)) {
                throw new ValidationDomainException(
                        "FORBIDDEN_DLT_TARGET",
                        "Replaying into dead-letter topic '" + trimmed + "' is blocked to prevent replay loops");
            }
        }
    }

    public static boolean isExcludedFromDiscovery(String topic) {
        String lower = topic.toLowerCase(Locale.ROOT);
        return topic.startsWith("__") || lower.endsWith(".canary") || lower.endsWith("-canary");
    }
}
