package io.github.duhanguler.dltdeck.core.model;

import java.util.Objects;

/**
 * Immutable Kafka partition coordinates.
 */
public record MessageCoordinates(String topic, int partition, long offset) {

    public MessageCoordinates(String topic, int partition, long offset) {
        Objects.requireNonNull(topic, "topic must not be null");
        String trimmed = topic.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be >= 0");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        this.topic = trimmed;
        this.partition = partition;
        this.offset = offset;
    }

    public String format() {
        return topic + "[" + partition + "]@" + offset;
    }
}
