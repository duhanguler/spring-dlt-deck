package io.github.duhanguler.dltdeck.core.codec;

import io.github.duhanguler.dltdeck.core.model.PayloadFormat;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Default binary-safe payload codec supporting:
 * <ul>
 *   <li>Kafka Tombstones ({@code payload == null && !hasDeserializationException})</li>
 *   <li>Confluent Schema Registry Wire Format ({@code magic byte 0x00 + 4-byte big-endian schemaId}), marked {@code NOT_PATCHABLE}</li>
 *   <li>Strict UTF-8 JSON objects/arrays ({@code patchable = true})</li>
 *   <li>Plain UTF-8 text and arbitrary binary hex dumps ({@code patchable = false})</li>
 *   <li>CloudEvents ({@code ce_type}, {@code ce_id}, {@code ce_source}) header extraction</li>
 * </ul>
 */
public final class DefaultPayloadCodec implements PayloadCodec {

    private static final int MAX_HEX_PREVIEW_BYTES = 512;
    private static final int MAX_TEXT_PREVIEW_CHARS = 16_384;
    private static final HexFormat HEX = HexFormat.ofDelimiter(" ").withUpperCase();

    private final JsonMapper jsonMapper;

    public DefaultPayloadCodec() {
        this(JsonMapper.builder().build());
    }

    public DefaultPayloadCodec(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    @Override
    public DecodedPayload decode(
            byte @Nullable [] payload, Map<String, String> headers, boolean hasDeserializationException) {
        String ceType = findHeaderIgnoreCase(headers, "ce_type");
        String ceId = findHeaderIgnoreCase(headers, "ce_id");
        String ceSource = findHeaderIgnoreCase(headers, "ce_source");

        if (payload == null) {
            if (!hasDeserializationException) {
                return new DecodedPayload(
                        PayloadFormat.TOMBSTONE, false, null, 0, "<TOMBSTONE: null payload>", ceType, ceId, ceSource);
            }
            return new DecodedPayload(
                    PayloadFormat.BINARY_HEX,
                    false,
                    null,
                    0,
                    "<EMPTY: null payload with deserialization error>",
                    ceType,
                    ceId,
                    ceSource);
        }

        int sizeBytes = payload.length;
        if (sizeBytes == 0) {
            return new DecodedPayload(PayloadFormat.UTF8_TEXT, false, null, 0, "", ceType, ceId, ceSource);
        }

        // Confluent Schema Registry Wire Format: magic byte 0x00 + 4-byte big-endian int schema ID (> 0)
        if (sizeBytes >= 5 && payload[0] == 0x00) {
            int schemaId = ByteBuffer.wrap(payload, 1, 4).getInt();
            if (schemaId > 0) {
                String preview = "<CONFLUENT_WIRE_FORMAT schemaId=" + schemaId + " bytes=" + sizeBytes
                        + " NOT_PATCHABLE>\n" + hexPreview(payload);
                return new DecodedPayload(
                        PayloadFormat.CONFLUENT_SCHEMA_REGISTRY,
                        false,
                        schemaId,
                        sizeBytes,
                        preview,
                        ceType,
                        ceId,
                        ceSource);
            }
        }

        String utf8 = decodeStrictUtf8(payload);
        if (utf8 != null && !containsBinaryControlChars(utf8)) {
            String trimmed = utf8.trim();
            if ((trimmed.startsWith("{") && trimmed.endsWith("}"))
                    || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
                if (isValidJson(trimmed)) {
                    return new DecodedPayload(
                            PayloadFormat.JSON,
                            true,
                            null,
                            sizeBytes,
                            truncateText(utf8),
                            ceType,
                            ceId,
                            ceSource);
                }
            }
            return new DecodedPayload(
                    PayloadFormat.UTF8_TEXT,
                    false,
                    null,
                    sizeBytes,
                    truncateText(utf8),
                    ceType,
                    ceId,
                    ceSource);
        }

        return new DecodedPayload(
                PayloadFormat.BINARY_HEX,
                false,
                null,
                sizeBytes,
                "<BINARY bytes=" + sizeBytes + " NOT_PATCHABLE>\n" + hexPreview(payload),
                ceType,
                ceId,
                ceSource);
    }

    private boolean isValidJson(String candidate) {
        try {
            JsonNode node = jsonMapper.readTree(candidate);
            return node != null && (node.isObject() || node.isArray());
        } catch (JacksonException ex) {
            return false;
        }
    }

    private static @Nullable String decodeStrictUtf8(byte[] bytes) {
        CharsetDecoder decoder = StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            CharBuffer decoded = decoder.decode(ByteBuffer.wrap(bytes));
            return decoded.toString();
        } catch (CharacterCodingException ex) {
            return null;
        }
    }

    private static boolean containsBinaryControlChars(String text) {
        int limit = Math.min(text.length(), 4096);
        for (int i = 0; i < limit; i++) {
            char c = text.charAt(i);
            if (c < 0x20 && c != '\n' && c != '\r' && c != '\t') {
                return true;
            }
        }
        return false;
    }

    private static String hexPreview(byte[] bytes) {
        int len = Math.min(bytes.length, MAX_HEX_PREVIEW_BYTES);
        String formatted = HEX.formatHex(bytes, 0, len);
        if (bytes.length > MAX_HEX_PREVIEW_BYTES) {
            return formatted + " ... (+" + (bytes.length - MAX_HEX_PREVIEW_BYTES) + " bytes)";
        }
        return formatted;
    }

    private static String truncateText(String text) {
        if (text.length() <= MAX_TEXT_PREVIEW_CHARS) {
            return text;
        }
        return text.substring(0, MAX_TEXT_PREVIEW_CHARS)
                + "\n... [truncated " + (text.length() - MAX_TEXT_PREVIEW_CHARS) + " chars]";
    }

    private static @Nullable String findHeaderIgnoreCase(Map<String, String> headers, String targetKey) {
        String direct = headers.get(targetKey);
        if (direct != null) {
            return direct;
        }
        String lower = targetKey.toLowerCase(Locale.ROOT);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if (entry.getKey().toLowerCase(Locale.ROOT).equals(lower)) {
                return entry.getValue();
            }
        }
        return null;
    }
}
