package io.github.duhanguler.dltdeck.mcp;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.json.JsonMapper;

/**
 * JSON-Schema-documented fail-closed MCP tool registry for {@code spring-dlt-deck}.
 * Exposes strictly 4 non-privileged tools: {@code list_dlt_clusters}, {@code inspect_dlt_message},
 * {@code dry_run_dlt_patch}, and {@code draft_replay_plan}.
 */
public final class DltDeckMcpToolRegistry {

    public static final String TOOL_LIST_CLUSTERS = "list_dlt_clusters";
    public static final String TOOL_INSPECT_MESSAGE = "inspect_dlt_message";
    public static final String TOOL_DRY_RUN_PATCH = "dry_run_dlt_patch";
    public static final String TOOL_DRAFT_PLAN = "draft_replay_plan";

    private final DltDeckMcpService mcpService;
    private final JsonMapper jsonMapper;

    public DltDeckMcpToolRegistry(DltDeckMcpService mcpService, JsonMapper jsonMapper) {
        this.mcpService = mcpService;
        this.jsonMapper = jsonMapper;
    }

    public List<ToolDescriptor> listTools() {
        return List.of(
                new ToolDescriptor(
                        TOOL_LIST_CLUSTERS,
                        "List deduplicated Kafka DLT exception clusters with failure categories and operator guidance (PII-redacted).",
                        Map.of(
                                "type", "object",
                                "properties", Map.of(
                                        "dltTopic", Map.of("type", "string"),
                                        "limit", Map.of("type", "integer", "default", 20)))),
                new ToolDescriptor(
                        TOOL_INSPECT_MESSAGE,
                        "Inspect a single DLT message (always PII-redacted; binary Confluent 0x00 marked NOT_PATCHABLE).",
                        Map.of(
                                "type", "object",
                                "required", List.of("messageId"),
                                "properties", Map.of("messageId", Map.of("type", "string", "format", "uuid")))),
                new ToolDescriptor(
                        TOOL_DRY_RUN_PATCH,
                        "Run a side-effect-free Tier-1 validation dry-run with an optional RFC 7396 JSON Merge Patch.",
                        Map.of(
                                "type", "object",
                                "required", List.of("messageId"),
                                "properties", Map.of(
                                        "messageId", Map.of("type", "string", "format", "uuid"),
                                        "mergePatchJson", Map.of("type", "string")))),
                new ToolDescriptor(
                        TOOL_DRAFT_PLAN,
                        "Draft a PENDING_APPROVAL key-ordered replay plan with frozen planSpecHash for human APPROVER review.",
                        Map.of(
                                "type", "object",
                                "required", List.of("name", "targetTopic"),
                                "properties", Map.of(
                                        "name", Map.of("type", "string"),
                                        "dltTopicFilter", Map.of("type", "string"),
                                        "fingerprintFilter", Map.of("type", "string"),
                                        "targetTopic", Map.of("type", "string"),
                                        "payloadPatchJson", Map.of("type", "string"),
                                        "rateLimitPerSec", Map.of("type", "integer", "default", 50),
                                        "maxRecords", Map.of("type", "integer", "default", 100)))));
    }

    public String invokeTool(String toolName, Map<String, Object> arguments) {
        Object result = switch (toolName) {
            case TOOL_LIST_CLUSTERS -> mcpService.listDltClusters(
                    stringArg(arguments, "dltTopic"), intArg(arguments, "limit", 20));
            case TOOL_INSPECT_MESSAGE -> mcpService.inspectDltMessage(requireUuid(arguments, "messageId"));
            case TOOL_DRY_RUN_PATCH -> mcpService.dryRunDltPatch(
                    requireUuid(arguments, "messageId"), stringArg(arguments, "mergePatchJson"));
            case TOOL_DRAFT_PLAN -> mcpService.draftReplayPlan(
                    requireString(arguments, "name"),
                    stringArg(arguments, "dltTopicFilter"),
                    stringArg(arguments, "fingerprintFilter"),
                    requireString(arguments, "targetTopic"),
                    stringArg(arguments, "payloadPatchJson"),
                    intArg(arguments, "rateLimitPerSec", 50),
                    intArg(arguments, "maxRecords", 100));
            default -> throw new IllegalArgumentException(
                    "Unknown or forbidden MCP tool '" + toolName + "' (fail-closed HITL policy)");
        };
        return jsonMapper.writeValueAsString(result);
    }

    public Map<String, Object> handleJsonRpc(Map<String, Object> request) {
        Object id = request.get("id");
        String method = String.valueOf(request.getOrDefault("method", ""));
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("jsonrpc", "2.0");
        if (id != null) {
            response.put("id", id);
        }
        if ("tools/list".equals(method)) {
            response.put("result", Map.of("tools", listTools()));
            return response;
        }
        if ("tools/call".equals(method)) {
            Object paramsObj = request.get("params");
            if (paramsObj instanceof Map<?, ?> params) {
                String name = String.valueOf(params.get("name"));
                @SuppressWarnings("unchecked")
                Map<String, Object> args = params.get("arguments") instanceof Map<?, ?> m
                        ? (Map<String, Object>) m
                        : Map.of();
                String text = invokeTool(name, args);
                response.put("result", Map.of("content", List.of(Map.of("type", "text", "text", text))));
                return response;
            }
        }
        response.put("error", Map.of("code", -32601, "message", "Unsupported MCP method: " + method));
        return response;
    }

    private static @Nullable String stringArg(Map<String, Object> args, String key) {
        Object v = args.get(key);
        return v == null ? null : String.valueOf(v);
    }

    private static String requireString(Map<String, Object> args, String key) {
        String s = stringArg(args, key);
        if (s == null || s.isBlank()) {
            throw new IllegalArgumentException("Missing required argument: " + key);
        }
        return s;
    }

    private static UUID requireUuid(Map<String, Object> args, String key) {
        return UUID.fromString(requireString(args, key));
    }

    private static int intArg(Map<String, Object> args, String key, int defaultValue) {
        Object v = args.get(key);
        if (v instanceof Number n) {
            return n.intValue();
        }
        if (v instanceof String s && !s.isBlank()) {
            return Integer.parseInt(s.trim());
        }
        return defaultValue;
    }

    public record ToolDescriptor(String name, String description, Map<String, Object> inputSchema) {}
}
