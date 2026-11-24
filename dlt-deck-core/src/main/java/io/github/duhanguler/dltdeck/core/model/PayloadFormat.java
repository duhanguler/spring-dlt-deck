package io.github.duhanguler.dltdeck.core.model;

public enum PayloadFormat {
    JSON(true),
    CONFLUENT_SCHEMA_REGISTRY(false),
    UTF8_TEXT(false),
    BINARY_HEX(false),
    TOMBSTONE(false);

    private final boolean patchable;

    PayloadFormat(boolean patchable) {
        this.patchable = patchable;
    }

    public boolean isPatchable() {
        return patchable;
    }
}
