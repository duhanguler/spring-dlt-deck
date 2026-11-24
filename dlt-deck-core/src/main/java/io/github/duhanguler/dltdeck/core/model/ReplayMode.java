package io.github.duhanguler.dltdeck.core.model;

import java.util.Objects;

/**
 * Exhaustive sealed replay strategy hierarchy.
 */
public sealed interface ReplayMode
        permits ReplayMode.OriginalTopic,
                ReplayMode.TargetTopic,
                ReplayMode.CanaryTopic,
                ReplayMode.PatchedPayload {

    record OriginalTopic() implements ReplayMode {}

    record TargetTopic(String targetTopic) implements ReplayMode {
        public TargetTopic(String targetTopic) {
            Objects.requireNonNull(targetTopic, "targetTopic must not be null");
            String trimmed = targetTopic.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalArgumentException("targetTopic must not be blank");
            }
            this.targetTopic = trimmed;
        }
    }

    record CanaryTopic(String canaryTopic) implements ReplayMode {
        public CanaryTopic(String canaryTopic) {
            Objects.requireNonNull(canaryTopic, "canaryTopic must not be null");
            String trimmed = canaryTopic.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalArgumentException("canaryTopic must not be blank");
            }
            this.canaryTopic = trimmed;
        }
    }

    record PatchedPayload(String targetTopic, String mergePatchJson) implements ReplayMode {
        public PatchedPayload(String targetTopic, String mergePatchJson) {
            Objects.requireNonNull(targetTopic, "targetTopic must not be null");
            Objects.requireNonNull(mergePatchJson, "mergePatchJson must not be null");
            String trimmedTopic = targetTopic.trim();
            if (trimmedTopic.isEmpty()) {
                throw new IllegalArgumentException("targetTopic must not be blank");
            }
            if (mergePatchJson.isBlank()) {
                throw new IllegalArgumentException("mergePatchJson must not be blank");
            }
            this.targetTopic = trimmedTopic;
            this.mergePatchJson = mergePatchJson;
        }
    }
}
