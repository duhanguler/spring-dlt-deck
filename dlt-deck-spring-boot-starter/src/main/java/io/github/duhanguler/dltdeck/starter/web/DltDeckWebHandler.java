package io.github.duhanguler.dltdeck.starter.web;

import io.github.duhanguler.dltdeck.core.exception.DltDeckException;
import io.github.duhanguler.dltdeck.core.exception.ForbiddenActionException;
import io.github.duhanguler.dltdeck.core.exception.NotFoundException;
import io.github.duhanguler.dltdeck.core.exception.PlanConflictException;
import io.github.duhanguler.dltdeck.core.exception.RateLimitExceededException;
import io.github.duhanguler.dltdeck.core.exception.ValidationDomainException;
import io.github.duhanguler.dltdeck.core.model.DryRunTier;
import io.github.duhanguler.dltdeck.core.model.MessageStatus;
import io.github.duhanguler.dltdeck.core.model.ReplayOrderingMode;
import io.github.duhanguler.dltdeck.starter.config.DltDeckProperties;
import io.github.duhanguler.dltdeck.starter.security.StrictCspNonceFilter;
import io.github.duhanguler.dltdeck.starter.service.DltDeckControlPlaneService;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Isolated WebMvc.fn handler backed by a private Jackson 3 {@link JsonMapper}.
 * Returns RFC 9457 {@link ProblemDetail} on all domain/security/validation errors and never touches
 * host {@code WebMvcConfigurer} or host {@code ObjectMapper} beans.
 */
public final class DltDeckWebHandler {

    private static final MediaType PROBLEM_JSON = MediaType.APPLICATION_PROBLEM_JSON;
    private static final MediaType TEXT_CSV = MediaType.parseMediaType("text/csv;charset=UTF-8");

    private final DltDeckControlPlaneService service;
    private final DltDeckProperties properties;
    private final JsonMapper privateJsonMapper;
    private final String uiHtmlTemplate;

    public DltDeckWebHandler(
            DltDeckControlPlaneService service, DltDeckProperties properties, JsonMapper privateJsonMapper) {
        this.service = service;
        this.properties = properties;
        this.privateJsonMapper = privateJsonMapper;
        this.uiHtmlTemplate = loadUiTemplate();
    }

    public ServerResponse serveUi(ServerRequest request) {
        if (!properties.getUi().isEnabled()) {
            return problem(HttpStatus.NOT_FOUND, "UI_DISABLED", "Embedded workbench UI is disabled", request);
        }
        Object nonceAttr = request.servletRequest().getAttribute(StrictCspNonceFilter.CSP_NONCE_ATTRIBUTE);
        String nonce = nonceAttr instanceof String s ? s : "";
        String html = uiHtmlTemplate
                .replace("{{CSP_NONCE}}", nonce)
                .replace("{{BASE_PATH}}", properties.getBasePath())
                .replace("{{TITLE}}", properties.getUi().getTitle());
        return ServerResponse.ok()
                .contentType(MediaType.TEXT_HTML)
                .header(HttpHeaders.CACHE_CONTROL, "no-store")
                .body(html);
    }

    public ServerResponse overview(ServerRequest request) {
        return guarded(request, () -> jsonOk(service.overview()));
    }

    public ServerResponse listClusters(ServerRequest request) {
        return guarded(request, () -> {
            String topic = request.param("dltTopic").orElse(null);
            int limit = parseIntParam(request, "limit", 50);
            return jsonOk(service.listClusters(topic, limit));
        });
    }

    public ServerResponse listMessages(ServerRequest request) {
        return guarded(request, () -> {
            String topic = request.param("dltTopic").orElse(null);
            String fp = request.param("fingerprint").orElse(null);
            String statusRaw = request.param("status").orElse(null);
            MessageStatus status = null;
            if (statusRaw != null && !statusRaw.isBlank()) {
                try {
                    status = MessageStatus.valueOf(statusRaw.trim().toUpperCase(Locale.ROOT));
                } catch (IllegalArgumentException ex) {
                    throw new ValidationDomainException("INVALID_STATUS", "Unknown message status: " + statusRaw);
                }
            }
            String cursor = request.param("cursor").orElse(null);
            int limit = parseIntParam(request, "limit", 50);
            return jsonOk(service.listMessages(topic, fp, status, cursor, limit));
        });
    }

    public ServerResponse inspectMessage(ServerRequest request) {
        return guarded(request, () -> {
            UUID id = parseUuidPath(request, "id");
            boolean unmask = Boolean.parseBoolean(request.param("unmask").orElse("false"));
            return jsonOk(service.inspectMessage(id, unmask));
        });
    }

    public ServerResponse previewPatch(ServerRequest request) {
        return guarded(request, () -> {
            UUID id = parseUuidPath(request, "id");
            PatchPreviewBody body = readJsonBody(request, PatchPreviewBody.class);
            if (body.mergePatchJson() == null || body.mergePatchJson().isBlank()) {
                throw new ValidationDomainException("PATCH_REQUIRED", "mergePatchJson is required");
            }
            return jsonOk(service.previewPatch(id, body.mergePatchJson()));
        });
    }

    public ServerResponse dryRun(ServerRequest request) {
        return guarded(request, () -> {
            UUID id = parseUuidPath(request, "id");
            DryRunBody body = readJsonBody(request, DryRunBody.class);
            DryRunTier tier = body.tier() == null ? DryRunTier.TIER_1_VALIDATE : body.tier();
            return jsonOk(service.dryRun(id, tier, body.targetTopic(), body.mergePatchJson()));
        });
    }

    public ServerResponse discardMessage(ServerRequest request) {
        return guarded(request, () -> {
            UUID id = parseUuidPath(request, "id");
            ReasonBody body = readOptionalJsonBody(request, ReasonBody.class, new ReasonBody("Operator discard"));
            service.discardMessage(id, body.reason() == null ? "Operator discard" : body.reason());
            return jsonOk(java.util.Map.of("status", "DISCARDED", "messageId", id));
        });
    }

    public ServerResponse resolveIndeterminate(ServerRequest request) {
        return guarded(request, () -> {
            UUID id = parseUuidPath(request, "id");
            ResolveBody body = readJsonBody(request, ResolveBody.class);
            if (body.targetStatus() == null) {
                throw new ValidationDomainException("TARGET_STATUS_REQUIRED", "targetStatus is required");
            }
            service.resolveIndeterminate(
                    id, body.targetStatus(), body.justification() == null ? "Approved manual resolution" : body.justification());
            return jsonOk(java.util.Map.of("messageId", id, "status", body.targetStatus().name()));
        });
    }

    public ServerResponse exportCsv(ServerRequest request) {
        return guarded(request, () -> {
            String topic = request.param("dltTopic").orElse(null);
            String fp = request.param("fingerprint").orElse(null);
            int limit = parseIntParam(request, "limit", 200);
            String csv = service.exportMessagesCsv(topic, fp, limit);
            return ServerResponse.ok()
                    .contentType(TEXT_CSV)
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"dlt-deck-export.csv\"")
                    .header(HttpHeaders.CACHE_CONTROL, "no-store")
                    .body(csv);
        });
    }

    public ServerResponse listPlans(ServerRequest request) {
        return guarded(request, () -> jsonOk(service.listPlans(parseIntParam(request, "limit", 50))));
    }

    public ServerResponse getPlan(ServerRequest request) {
        return guarded(request, () -> jsonOk(service.getPlan(parseUuidPath(request, "id"))));
    }

    public ServerResponse draftPlan(ServerRequest request) {
        return guarded(request, () -> {
            DraftPlanBody body = readJsonBody(request, DraftPlanBody.class);
            if (body.name() == null || body.targetTopic() == null) {
                throw new ValidationDomainException("INVALID_PLAN_REQUEST", "name and targetTopic are required");
            }
            var plan = service.draftReplayPlan(new DltDeckControlPlaneService.DraftPlanRequest(
                    body.name(),
                    body.fingerprintFilter(),
                    body.dltTopicFilter(),
                    body.targetTopic(),
                    body.orderingMode(),
                    body.payloadPatchJson(),
                    body.rateLimitPerSec(),
                    body.maxRecords()));
            return jsonStatus(HttpStatus.CREATED, plan);
        });
    }

    public ServerResponse approvePlan(ServerRequest request) {
        return guarded(request, () -> {
            UUID id = parseUuidPath(request, "id");
            ApprovePlanBody body = readOptionalJsonBody(request, ApprovePlanBody.class, new ApprovePlanBody(null, true));
            boolean async = body.executeAsync() == null || body.executeAsync();
            return jsonOk(service.approveReplayPlan(id, body.expectedSpecHash(), async));
        });
    }

    public ServerResponse rejectPlan(ServerRequest request) {
        return guarded(request, () -> {
            UUID id = parseUuidPath(request, "id");
            ReasonBody body = readOptionalJsonBody(request, ReasonBody.class, new ReasonBody("Rejected by approver"));
            return jsonOk(service.rejectReplayPlan(id, body.reason() == null ? "Rejected" : body.reason()));
        });
    }

    public ServerResponse listAudit(ServerRequest request) {
        return guarded(request, () -> jsonOk(service.listAuditEntries(parseIntParam(request, "limit", 100))));
    }

    public ServerResponse verifyAudit(ServerRequest request) {
        return guarded(request, () -> jsonOk(service.verifyAuditChain()));
    }

    public ServerResponse sseStream(ServerRequest request) {
        return ServerResponse.sse(sse -> {
            try {
                String payload = privateJsonMapper.writeValueAsString(service.overview());
                sse.event("overview").data(payload);
                sse.complete();
            } catch (Exception ex) {
                sse.error(ex);
            }
        });
    }

    private ServerResponse guarded(ServerRequest request, ResponseSupplier supplier) {
        try {
            return supplier.get();
        } catch (ForbiddenActionException ex) {
            return problem(HttpStatus.FORBIDDEN, ex.errorCode(), ex.getMessage(), request);
        } catch (NotFoundException ex) {
            return problem(HttpStatus.NOT_FOUND, ex.errorCode(), ex.getMessage(), request);
        } catch (PlanConflictException ex) {
            return problem(HttpStatus.CONFLICT, ex.errorCode(), ex.getMessage(), request);
        } catch (ValidationDomainException ex) {
            return problem(HttpStatus.UNPROCESSABLE_CONTENT, ex.errorCode(), ex.getMessage(), request);
        } catch (RateLimitExceededException ex) {
            return problem(HttpStatus.TOO_MANY_REQUESTS, ex.errorCode(), ex.getMessage(), request);
        } catch (DltDeckException ex) {
            return problem(HttpStatus.BAD_REQUEST, ex.errorCode(), ex.getMessage(), request);
        } catch (IllegalArgumentException ex) {
            return problem(HttpStatus.BAD_REQUEST, "BAD_REQUEST", ex.getMessage(), request);
        } catch (Exception ex) {
            return problem(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Unexpected control-plane error", request);
        }
    }

    private ServerResponse jsonOk(Object body) {
        return jsonStatus(HttpStatus.OK, body);
    }

    private ServerResponse jsonStatus(HttpStatus status, Object body) {
        String json = privateJsonMapper.writeValueAsString(body);
        return ServerResponse.status(status)
                .contentType(MediaType.APPLICATION_JSON)
                .header(HttpHeaders.CACHE_CONTROL, "no-store")
                .body(json);
    }

    private ServerResponse problem(
            HttpStatus status, String errorCode, @Nullable String detail, ServerRequest request) {
        String resolvedDetail = detail == null ? status.getReasonPhrase() : detail;
        URI typeUri = URI.create("urn:dlt-deck:error:" + errorCode.toLowerCase(Locale.ROOT));
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(status, resolvedDetail);
        pd.setTitle(errorCode);
        pd.setType(typeUri);
        pd.setInstance(URI.create(request.path()));
        java.util.Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("type", typeUri.toString());
        body.put("title", errorCode);
        body.put("status", pd.getStatus());
        body.put("detail", resolvedDetail);
        body.put("instance", request.path());
        body.put("errorCode", errorCode);
        String json = privateJsonMapper.writeValueAsString(body);
        return ServerResponse.status(status)
                .contentType(PROBLEM_JSON)
                .header(HttpHeaders.CACHE_CONTROL, "no-store")
                .body(json);
    }

    private <T> T readJsonBody(ServerRequest request, Class<T> type) {
        try {
            byte[] bytes = request.servletRequest().getInputStream().readAllBytes();
            if (bytes.length == 0) {
                throw new ValidationDomainException("EMPTY_BODY", "Request body is required");
            }
            return privateJsonMapper.readValue(bytes, type);
        } catch (JacksonException ex) {
            throw new ValidationDomainException("INVALID_JSON", "Malformed JSON request body: " + ex.getOriginalMessage(), ex);
        } catch (IOException ex) {
            throw new ValidationDomainException("BODY_READ_FAILED", "Failed to read request body", ex);
        }
    }

    private <T> T readOptionalJsonBody(ServerRequest request, Class<T> type, T fallback) {
        try {
            byte[] bytes = request.servletRequest().getInputStream().readAllBytes();
            if (bytes.length == 0) {
                return fallback;
            }
            return privateJsonMapper.readValue(bytes, type);
        } catch (JacksonException ex) {
            throw new ValidationDomainException("INVALID_JSON", "Malformed JSON request body: " + ex.getOriginalMessage(), ex);
        } catch (IOException ex) {
            return fallback;
        }
    }

    private static UUID parseUuidPath(ServerRequest request, String name) {
        String raw = request.pathVariable(name);
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException ex) {
            throw new ValidationDomainException("INVALID_UUID", "Invalid UUID path variable '" + name + "': " + raw);
        }
    }

    private static int parseIntParam(ServerRequest request, String name, int defaultValue) {
        return request.param(name)
                .map(raw -> {
                    try {
                        return Integer.parseInt(raw.trim());
                    } catch (NumberFormatException ex) {
                        throw new ValidationDomainException("INVALID_PARAM", "Invalid integer parameter: " + name);
                    }
                })
                .orElse(defaultValue);
    }

    private static String loadUiTemplate() {
        try (InputStream in = DltDeckWebHandler.class.getClassLoader().getResourceAsStream("dltdeck/ui/index.html")) {
            if (in == null) {
                return "<!doctype html><html><body><h1>spring-dlt-deck</h1></body></html>";
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            return "<!doctype html><html><body><h1>spring-dlt-deck</h1></body></html>";
        }
    }

    @FunctionalInterface
    private interface ResponseSupplier {
        ServerResponse get();
    }

    public record PatchPreviewBody(@Nullable String mergePatchJson) {}

    public record DryRunBody(
            @Nullable DryRunTier tier, @Nullable String targetTopic, @Nullable String mergePatchJson) {}

    public record ReasonBody(@Nullable String reason) {}

    public record ResolveBody(@Nullable MessageStatus targetStatus, @Nullable String justification) {}

    public record DraftPlanBody(
            @Nullable String name,
            @Nullable String fingerprintFilter,
            @Nullable String dltTopicFilter,
            @Nullable String targetTopic,
            @Nullable ReplayOrderingMode orderingMode,
            @Nullable String payloadPatchJson,
            @Nullable Integer rateLimitPerSec,
            @Nullable Integer maxRecords) {}

    public record ApprovePlanBody(@Nullable String expectedSpecHash, @Nullable Boolean executeAsync) {}
}
