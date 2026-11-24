package io.github.duhanguler.dltdeck.core.spi;

import java.util.Optional;
import java.util.UUID;

/**
 * Pluggable cold payload blob spill SPI (e.g., S3/GCS/MinIO) for oversized DLT payloads.
 */
public interface PayloadBlobStore {

    String store(UUID messageId, byte[] payload);

    Optional<byte[]> load(String externalBlobUri);

    void delete(String externalBlobUri);
}
