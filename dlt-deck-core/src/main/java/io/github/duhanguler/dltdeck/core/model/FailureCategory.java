package io.github.duhanguler.dltdeck.core.model;

public enum FailureCategory {
    DESERIALIZATION("Inspect raw poison-pill bytes recovered from ErrorHandlingDeserializer; patch malformed JSON or skip binary records."),
    VALIDATION("Inspect payload fields against bean/schema constraints; apply an RFC 7396 patch and run Tier-1 Dry-Run before replay."),
    SCHEMA_MISMATCH("Verify producer/consumer contract and Confluent Schema Registry ID compatibility before replaying."),
    TRANSIENT_DOWNSTREAM("Verify downstream dependency health, then replay with rate limiting and blast-radius protection."),
    AUTH_PERMISSION("Rotate credentials or fix ACL/RBAC grants on target systems before replaying."),
    POISON_DATA("Data violates business invariants; inspect record carefully, patch if JSON, or discard with audit trail."),
    REPLAY_LOOP("Message exceeded maxReplayHops and has been quarantined; fix consumer bug before manual release."),
    UNKNOWN("Inspect canonical stacktrace and headers, validate with Dry-Run, and canary-test a single record first.");

    private final String defaultRecommendation;

    FailureCategory(String defaultRecommendation) {
        this.defaultRecommendation = defaultRecommendation;
    }

    public String defaultRecommendation() {
        return defaultRecommendation;
    }
}
