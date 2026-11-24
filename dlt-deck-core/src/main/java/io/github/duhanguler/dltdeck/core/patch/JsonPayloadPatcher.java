package io.github.duhanguler.dltdeck.core.patch;

import io.github.duhanguler.dltdeck.core.exception.ValidationDomainException;
import io.github.duhanguler.dltdeck.core.model.PayloadFormat;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Bounded RFC 7396 JSON Merge Patch engine built on Jackson 3.
 *
 * <p>Enforces three safety invariants:
 * <ol>
 *   <li>Refuses any non-JSON payload format ({@code NOT_PATCHABLE}) so Confluent Avro/Protobuf binary payloads cannot be corrupted.</li>
 *   <li>Enforces strict {@code maxPayloadBytes} (default 1 MiB) on input, patch, and output.</li>
 *   <li>Enforces strict {@code maxNestingDepth} (default 32) to prevent JSON parser/recursion bombs.</li>
 * </ol>
 */
public final class JsonPayloadPatcher {

    public static final int DEFAULT_MAX_PAYLOAD_BYTES = 1_048_576; // 1 MiB
    public static final int DEFAULT_MAX_NESTING_DEPTH = 32;

    private final JsonMapper jsonMapper;
    private final int maxPayloadBytes;
    private final int maxNestingDepth;

    public JsonPayloadPatcher() {
        this(JsonMapper.builder().build(), DEFAULT_MAX_PAYLOAD_BYTES, DEFAULT_MAX_NESTING_DEPTH);
    }

    public JsonPayloadPatcher(JsonMapper jsonMapper, int maxPayloadBytes, int maxNestingDepth) {
        if (maxPayloadBytes <= 0) {
            throw new IllegalArgumentException("maxPayloadBytes must be positive");
        }
        if (maxNestingDepth <= 0) {
            throw new IllegalArgumentException("maxNestingDepth must be positive");
        }
        this.jsonMapper = jsonMapper;
        this.maxPayloadBytes = maxPayloadBytes;
        this.maxNestingDepth = maxNestingDepth;
    }

    public PatchResult applyMergePatch(
            PayloadFormat format, byte @Nullable [] originalPayload, String mergePatchJson) {
        if (!format.isPatchable()) {
            throw new ValidationDomainException(
                    "NOT_PATCHABLE",
                    "Payload format " + format + " is NOT_PATCHABLE; RFC 7396 patching only applies to JSON payloads");
        }
        if (originalPayload == null || originalPayload.length == 0) {
            throw new ValidationDomainException("EMPTY_PAYLOAD", "Cannot apply JSON merge patch to empty payload");
        }
        if (originalPayload.length > maxPayloadBytes) {
            throw new ValidationDomainException(
                    "PAYLOAD_TOO_LARGE",
                    "Original payload exceeds maximum patchable size of " + maxPayloadBytes + " bytes");
        }
        byte[] patchBytes = mergePatchJson.getBytes(StandardCharsets.UTF_8);
        if (patchBytes.length > maxPayloadBytes) {
            throw new ValidationDomainException(
                    "PATCH_TOO_LARGE",
                    "Merge patch exceeds maximum allowed size of " + maxPayloadBytes + " bytes");
        }

        validateDepthRawJson(originalPayload, "original payload");
        validateDepthRawJson(patchBytes, "merge patch");

        JsonNode targetNode = parseTree(originalPayload, "INVALID_ORIGINAL_JSON");
        JsonNode patchNode = parseTree(patchBytes, "INVALID_PATCH_JSON");

        verifyNodeDepth(targetNode, 0, "original payload");
        verifyNodeDepth(patchNode, 0, "merge patch");

        JsonNode merged = merge(targetNode.deepCopy(), patchNode);
        verifyNodeDepth(merged, 0, "merged payload");

        byte[] mergedBytes;
        String prettyMerged;
        try {
            mergedBytes = jsonMapper.writeValueAsBytes(merged);
            prettyMerged = jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(merged);
        } catch (JacksonException ex) {
            throw new ValidationDomainException("SERIALIZATION_FAILED", "Failed to serialize patched JSON", ex);
        }

        if (mergedBytes.length > maxPayloadBytes) {
            throw new ValidationDomainException(
                    "PATCHED_PAYLOAD_TOO_LARGE",
                    "Patched payload exceeds maximum allowed size of " + maxPayloadBytes + " bytes");
        }

        List<DiffEntry> diffs = new ArrayList<>();
        collectDiffs("$", targetNode, merged, diffs);
        return new PatchResult(mergedBytes, prettyMerged, diffs);
    }

    private JsonNode parseTree(byte[] bytes, String errorCode) {
        try {
            JsonNode node = jsonMapper.readTree(bytes);
            if (node == null) {
                throw new ValidationDomainException(errorCode, "Parsed JSON document was empty");
            }
            return node;
        } catch (JacksonException ex) {
            throw new ValidationDomainException(errorCode, "Invalid JSON syntax: " + ex.getOriginalMessage(), ex);
        }
    }

    /**
     * Fast pre-parse lexical bracket depth guard so malicious deeply-nested payloads fail before Jackson recursion.
     */
    private void validateDepthRawJson(byte[] utf8Bytes, String label) {
        int depth = 0;
        boolean inString = false;
        boolean escaped = false;
        for (byte b : utf8Bytes) {
            if (inString) {
                if (escaped) {
                    escaped = false;
                } else if (b == '\\') {
                    escaped = true;
                } else if (b == '"') {
                    inString = false;
                }
                continue;
            }
            if (b == '"') {
                inString = true;
            } else if (b == '{' || b == '[') {
                depth++;
                if (depth > maxNestingDepth) {
                    throw new ValidationDomainException(
                            "JSON_DEPTH_EXCEEDED",
                            "JSON nesting depth in " + label + " exceeds maximum limit of " + maxNestingDepth);
                }
            } else if (b == '}' || b == ']') {
                depth = Math.max(0, depth - 1);
            }
        }
    }

    private void verifyNodeDepth(JsonNode node, int currentDepth, String label) {
        if (currentDepth > maxNestingDepth) {
            throw new ValidationDomainException(
                    "JSON_DEPTH_EXCEEDED",
                    "JSON nesting depth in " + label + " exceeds maximum limit of " + maxNestingDepth);
        }
        if (node.isObject()) {
            for (Map.Entry<String, JsonNode> entry : node.properties()) {
                verifyNodeDepth(entry.getValue(), currentDepth + 1, label);
            }
        } else if (node.isArray()) {
            for (JsonNode child : node) {
                verifyNodeDepth(child, currentDepth + 1, label);
            }
        }
    }

    /**
     * Implements RFC 7396 Section 2 MergePatch(Target, Patch).
     */
    private JsonNode merge(JsonNode target, JsonNode patch) {
        if (!patch.isObject()) {
            return patch.deepCopy();
        }
        ObjectNode targetObj = target.isObject() ? (ObjectNode) target : jsonMapper.createObjectNode();
        for (Map.Entry<String, JsonNode> entry : patch.properties()) {
            String field = entry.getKey();
            JsonNode value = entry.getValue();
            if (value.isNull()) {
                targetObj.remove(field);
            } else {
                JsonNode existing = targetObj.get(field);
                if (existing != null && existing.isObject() && value.isObject()) {
                    targetObj.set(field, merge(existing, value));
                } else {
                    targetObj.set(field, merge(jsonMapper.createObjectNode(), value));
                }
            }
        }
        return targetObj;
    }

    private void collectDiffs(String path, @Nullable JsonNode before, @Nullable JsonNode after, List<DiffEntry> diffs) {
        if (before == null) {
            if (after != null) {
                diffs.add(new DiffEntry(path, DiffOp.ADDED, null, after.toString()));
            }
            return;
        }
        if (after == null) {
            diffs.add(new DiffEntry(path, DiffOp.REMOVED, before.toString(), null));
            return;
        }
        if (before.equals(after)) {
            return;
        }
        if (before.isObject() && after.isObject()) {
            Set<String> keys = new LinkedHashSet<>();
            before.propertyNames().forEach(keys::add);
            after.propertyNames().forEach(keys::add);
            for (String key : keys) {
                collectDiffs(path + "." + key, before.get(key), after.get(key), diffs);
            }
            return;
        }
        diffs.add(new DiffEntry(path, DiffOp.MODIFIED, before.toString(), after.toString()));
    }

    public enum DiffOp {
        ADDED,
        REMOVED,
        MODIFIED
    }

    public record DiffEntry(
            String jsonPath,
            DiffOp operation,
            @Nullable String beforeValue,
            @Nullable String afterValue) {}

    @SuppressWarnings("ArrayRecordComponent")
    public record PatchResult(byte[] patchedBytes, String prettyJson, List<DiffEntry> diffs) {
        public PatchResult {
            patchedBytes = patchedBytes.clone();
            diffs = List.copyOf(diffs);
        }

        @Override
        public byte[] patchedBytes() {
            return patchedBytes.clone();
        }
    }
}
