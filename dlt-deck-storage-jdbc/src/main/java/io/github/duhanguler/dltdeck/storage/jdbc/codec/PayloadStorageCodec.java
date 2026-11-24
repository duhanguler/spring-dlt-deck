package io.github.duhanguler.dltdeck.storage.jdbc.codec;

import io.github.duhanguler.dltdeck.core.spi.PayloadBlobStore;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.jspecify.annotations.Nullable;

/**
 * Cold payload storage codec supporting bounded size capping, optional GZIP compression,
 * and pluggable external {@link PayloadBlobStore} spill (e.g., S3/MinIO).
 */
public final class PayloadStorageCodec {

    public static final String ENCODING_RAW = "RAW";
    public static final String ENCODING_GZIP = "GZIP";
    public static final String ENCODING_EXTERNAL = "EXTERNAL_URI";

    private final int maxPayloadBytes;
    private final int compressionThresholdBytes;
    private final boolean gzipEnabled;
    private final @Nullable PayloadBlobStore blobStore;

    public PayloadStorageCodec(
            int maxPayloadBytes,
            int compressionThresholdBytes,
            boolean gzipEnabled,
            @Nullable PayloadBlobStore blobStore) {
        this.maxPayloadBytes = maxPayloadBytes;
        this.compressionThresholdBytes = compressionThresholdBytes;
        this.gzipEnabled = gzipEnabled;
        this.blobStore = blobStore;
    }

    public EncodedPayload encode(UUID messageId, byte @Nullable [] rawPayload) {
        if (rawPayload == null) {
            return new EncodedPayload(null, ENCODING_RAW, null);
        }
        byte[] bounded = rawPayload.length > maxPayloadBytes
                ? Arrays.copyOf(rawPayload, maxPayloadBytes)
                : rawPayload;

        if (blobStore != null && bounded.length > compressionThresholdBytes) {
            String uri = blobStore.store(messageId, bounded);
            return new EncodedPayload(null, ENCODING_EXTERNAL, uri);
        }
        if (gzipEnabled && bounded.length >= compressionThresholdBytes) {
            byte[] compressed = gzip(bounded);
            if (compressed.length < bounded.length) {
                return new EncodedPayload(compressed, ENCODING_GZIP, null);
            }
        }
        return new EncodedPayload(bounded, ENCODING_RAW, null);
    }

    public byte @Nullable [] decode(
            byte @Nullable [] storedBytes, String encoding, @Nullable String externalBlobUri) {
        if (ENCODING_EXTERNAL.equals(encoding) && externalBlobUri != null && blobStore != null) {
            return blobStore.load(externalBlobUri).orElse(null);
        }
        if (storedBytes == null) {
            return null;
        }
        if (ENCODING_GZIP.equals(encoding)) {
            return gunzip(storedBytes);
        }
        return storedBytes;
    }

    private static byte[] gzip(byte[] data) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                GZIPOutputStream gos = new GZIPOutputStream(baos)) {
            gos.write(data);
            gos.finish();
            return baos.toByteArray();
        } catch (IOException ex) {
            throw new IllegalStateException("GZIP compression failed", ex);
        }
    }

    private static byte[] gunzip(byte[] compressed) {
        try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
            return gis.readAllBytes();
        } catch (IOException ex) {
            throw new IllegalStateException("GZIP decompression failed", ex);
        }
    }

    @SuppressWarnings("ArrayRecordComponent")
    public record EncodedPayload(
            byte @Nullable [] bytes, String encoding, @Nullable String externalBlobUri) {
        public EncodedPayload(byte @Nullable [] bytes, String encoding, @Nullable String externalBlobUri) {
            this.bytes = bytes;
            this.encoding = encoding;
            this.externalBlobUri = externalBlobUri;
        }
    }
}
