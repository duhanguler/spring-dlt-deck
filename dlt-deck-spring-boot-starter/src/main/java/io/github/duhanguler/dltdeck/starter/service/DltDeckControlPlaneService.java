package io.github.duhanguler.dltdeck.starter.service;

import io.github.duhanguler.dltdeck.core.codec.PayloadCodec;
import io.github.duhanguler.dltdeck.core.context.ActorContext;
import io.github.duhanguler.dltdeck.core.exception.ForbiddenActionException;
import io.github.duhanguler.dltdeck.core.exception.NotFoundException;
import io.github.duhanguler.dltdeck.core.exception.PlanConflictException;
import io.github.duhanguler.dltdeck.core.exception.ValidationDomainException;
import io.github.duhanguler.dltdeck.core.masking.CsvExportSanitizer;
import io.github.duhanguler.dltdeck.core.masking.PiiRedactor;
import io.github.duhanguler.dltdeck.core.model.AuditChainVerification;
import io.github.duhanguler.dltdeck.core.model.AuditEntry;
import io.github.duhanguler.dltdeck.core.model.DltMessageDetail;
import io.github.duhanguler.dltdeck.core.model.DltMessageSummary;
import io.github.duhanguler.dltdeck.core.model.DryRunOutcome;
import io.github.duhanguler.dltdeck.core.model.DryRunTier;
import io.github.duhanguler.dltdeck.core.model.FingerprintCluster;
import io.github.duhanguler.dltdeck.core.model.KeysetPage;
import io.github.duhanguler.dltdeck.core.model.MessageStatus;
import io.github.duhanguler.dltdeck.core.model.ReplayOrderingMode;
import io.github.duhanguler.dltdeck.core.model.ReplayPlan;
import io.github.duhanguler.dltdeck.core.model.ReplayPlanStatus;
import io.github.duhanguler.dltdeck.core.patch.JsonPayloadPatcher;
import io.github.duhanguler.dltdeck.core.policy.MakerCheckerPolicy;
import io.github.duhanguler.dltdeck.core.spi.AuditLedgerRepository;
import io.github.duhanguler.dltdeck.core.spi.DltMessageRepository;
import io.github.duhanguler.dltdeck.core.spi.ReplayPlanRepository;
import io.github.duhanguler.dltdeck.starter.config.DltDeckProperties;
import io.github.duhanguler.dltdeck.starter.kafka.DltBatchIngestionEngine;
import io.github.duhanguler.dltdeck.starter.kafka.KeyStripedReplayExecutor;
import io.github.duhanguler.dltdeck.starter.kafka.TargetTopicValidator;
import io.github.duhanguler.dltdeck.starter.kafka.TieredDryRunService;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Unified governance, triage, dry-run, four-eyes approval, replay, and audit service.
 * Enforces {@link ActorContext} RBAC and tamper-evident audit logging for every privileged action.
 */
public final class DltDeckControlPlaneService {

    private final DltMessageRepository messageRepository;
    private final ReplayPlanRepository planRepository;
    private final AuditLedgerRepository auditRepository;
    private final DltBatchIngestionEngine ingestionEngine;
    private final KeyStripedReplayExecutor replayExecutor;
    private final TieredDryRunService dryRunService;
    private final TargetTopicValidator targetTopicValidator;
    private final PayloadCodec payloadCodec;
    private final PiiRedactor piiRedactor;
    private final JsonPayloadPatcher payloadPatcher;
    private final MakerCheckerPolicy makerCheckerPolicy;
    private final DltDeckProperties properties;
    private final JsonMapper jsonMapper;
    private final Clock clock;
    private final ExecutorService asyncPlanRunner = Executors.newVirtualThreadPerTaskExecutor();

    public DltDeckControlPlaneService(
            DltMessageRepository messageRepository,
            ReplayPlanRepository planRepository,
            AuditLedgerRepository auditRepository,
            DltBatchIngestionEngine ingestionEngine,
            KeyStripedReplayExecutor replayExecutor,
            TieredDryRunService dryRunService,
            TargetTopicValidator targetTopicValidator,
            PayloadCodec payloadCodec,
            PiiRedactor piiRedactor,
            JsonPayloadPatcher payloadPatcher,
            MakerCheckerPolicy makerCheckerPolicy,
            DltDeckProperties properties,
            JsonMapper jsonMapper,
            Clock clock) {
        this.messageRepository = messageRepository;
        this.planRepository = planRepository;
        this.auditRepository = auditRepository;
        this.ingestionEngine = ingestionEngine;
        this.replayExecutor = replayExecutor;
        this.dryRunService = dryRunService;
        this.targetTopicValidator = targetTopicValidator;
        this.payloadCodec = payloadCodec;
        this.piiRedactor = piiRedactor;
        this.payloadPatcher = payloadPatcher;
        this.makerCheckerPolicy = makerCheckerPolicy;
        this.properties = properties;
        this.jsonMapper = jsonMapper;
        this.clock = clock;
    }

    public OverviewSnapshot overview() {
        List<FingerprintCluster> clusters = messageRepository.listClusters(null, 50);
        long totalMessages = clusters.stream().mapToLong(FingerprintCluster::totalCount).sum();
        long actionableMessages = clusters.stream().mapToLong(FingerprintCluster::newCount).sum();
        List<ReplayPlan> plans = planRepository.listRecent(50);
        long pausedBlastRadius = plans.stream()
                .filter(p -> p.status() == ReplayPlanStatus.PAUSED_BLAST_RADIUS)
                .count();
        long pendingApprovals = plans.stream()
                .filter(p -> p.status() == ReplayPlanStatus.PENDING_APPROVAL)
                .count();
        int indeterminateCount = messageRepository
                .listMessages(null, null, MessageStatus.INDETERMINATE, null, 200)
                .items()
                .size();
        int blockedHolCount = messageRepository
                .listMessages(null, null, MessageStatus.BLOCKED_BY_PRIOR_KEY_FAILURE, null, 200)
                .items()
                .size();

        ActorContext actor = ActorContext.currentOrSystem();
        return new OverviewSnapshot(
                properties.getPodId(),
                actor.actorId(),
                actor.role().name(),
                ingestionEngine.isRunning(),
                ingestionEngine.totalIngestionLag(),
                ingestionEngine.ingestionLagByTopic(),
                ingestionEngine.subscribedTopics(),
                clusters.size(),
                totalMessages,
                actionableMessages,
                indeterminateCount,
                blockedHolCount,
                pendingApprovals,
                pausedBlastRadius,
                Instant.now(clock));
    }

    public List<FingerprintCluster> listClusters(@Nullable String dltTopic, int limit) {
        return messageRepository.listClusters(dltTopic, limit).stream()
                .map(c -> new FingerprintCluster(
                        c.fingerprint(),
                        c.exceptionFqcn(),
                        c.rootCauseFqcn(),
                        c.sampleMessage() == null ? null : piiRedactor.redactText(c.sampleMessage()),
                        c.canonicalStacktrace() == null ? null : piiRedactor.redactText(c.canonicalStacktrace()),
                        c.failureCategory(),
                        c.recommendation(),
                        c.firstSeenAt(),
                        c.lastSeenAt(),
                        c.totalCount(),
                        c.newCount()))
                .toList();
    }

    public KeysetPage<DltMessageSummary> listMessages(
            @Nullable String dltTopic,
            @Nullable String fingerprint,
            @Nullable MessageStatus status,
            @Nullable String cursor,
            int limit) {
        return messageRepository.listMessages(dltTopic, fingerprint, status, cursor, limit);
    }

    public InspectedMessage inspectMessage(UUID messageId, boolean unmaskPii) {
        ActorContext actor = ActorContext.currentOrSystem();
        if (unmaskPii && !actor.role().canUnmaskPii()) {
            throw new ForbiddenActionException(
                    "UNMASK_FORBIDDEN", "Only APPROVER actors may unmask PII on DLT records");
        }
        DltMessageDetail detail = messageRepository
                .findDetailById(messageId)
                .orElseThrow(() -> new NotFoundException("MESSAGE_NOT_FOUND", "Message not found: " + messageId));

        PayloadCodec.DecodedPayload decoded = payloadCodec.decode(detail.payload(), detail.headers(), false);
        String keyDisplay = detail.messageKey() == null
                ? null
                : new String(detail.messageKey(), StandardCharsets.UTF_8);

        String displayPayload;
        Map<String, String> displayHeaders;
        String displayKey;
        if (unmaskPii) {
            displayPayload = decoded.displayPreview();
            displayHeaders = detail.headers();
            displayKey = keyDisplay;
            audit("UNMASK_PII", "MESSAGE", messageId.toString(), Map.of("dltTopic", detail.summary().dltTopic()));
        } else {
            displayPayload = piiRedactor.redactText(decoded.displayPreview());
            displayHeaders = piiRedactor.redactHeaders(detail.headers());
            displayKey = keyDisplay == null ? null : piiRedactor.redactText(keyDisplay);
        }

        return new InspectedMessage(
                detail.summary(),
                detail.cluster().exceptionFqcn(),
                detail.cluster().rootCauseFqcn(),
                detail.cluster().sampleMessage() == null
                        ? null
                        : (unmaskPii ? detail.cluster().sampleMessage() : piiRedactor.redactText(detail.cluster().sampleMessage())),
                detail.cluster().canonicalStacktrace() == null
                        ? null
                        : (unmaskPii
                                ? detail.cluster().canonicalStacktrace()
                                : piiRedactor.redactText(detail.cluster().canonicalStacktrace())),
                detail.cluster().recommendation(),
                decoded.format(),
                decoded.patchable(),
                decoded.schemaId(),
                displayKey,
                displayPayload,
                displayHeaders,
                detail.cloudEventType(),
                detail.cloudEventId(),
                detail.cloudEventSource(),
                !unmaskPii);
    }

    public JsonPayloadPatcher.PatchResult previewPatch(UUID messageId, String mergePatchJson) {
        requireOperator();
        DltMessageDetail detail = messageRepository
                .findDetailById(messageId)
                .orElseThrow(() -> new NotFoundException("MESSAGE_NOT_FOUND", "Message not found: " + messageId));
        return payloadPatcher.applyMergePatch(detail.summary().payloadFormat(), detail.payload(), mergePatchJson);
    }

    public DryRunOutcome dryRun(
            UUID messageId, DryRunTier tier, @Nullable String targetTopic, @Nullable String mergePatchJson) {
        if (tier == DryRunTier.TIER_3_CANARY) {
            requireOperator();
        }
        DryRunOutcome outcome = dryRunService.executeDryRun(messageId, tier, targetTopic, mergePatchJson, null);
        audit(
                "DRY_RUN_" + tier.name(),
                "MESSAGE",
                messageId.toString(),
                Map.of("passed", outcome.passed(), "summary", outcome.summary()));
        return outcome;
    }

    public ReplayPlan draftReplayPlan(DraftPlanRequest request) {
        ActorContext actor = ActorContext.currentOrSystem();
        if (!actor.role().canOperate() && actor.role() != io.github.duhanguler.dltdeck.core.context.ActorRole.MCP_AGENT) {
            throw new ForbiddenActionException("DRAFT_FORBIDDEN", "Actor role " + actor.role() + " cannot draft replay plans");
        }
        if (request.name() == null || request.name().isBlank()) {
            throw new ValidationDomainException("INVALID_PLAN_NAME", "Replay plan name must not be blank");
        }
        targetTopicValidator.validateReplayTarget(request.targetTopic());

        int rateLimit = request.rateLimitPerSec() != null && request.rateLimitPerSec() > 0
                ? request.rateLimitPerSec()
                : properties.getReplay().getDefaultRateLimitPerSec();
        int maxRecords = request.maxRecords() != null && request.maxRecords() > 0
                ? request.maxRecords()
                : properties.getReplay().getDefaultMaxRecords();
        ReplayOrderingMode orderingMode = request.orderingMode() != null
                ? request.orderingMode()
                : ReplayOrderingMode.PRESERVE_KEY_ORDER;

        if (request.payloadPatchJson() != null && !request.payloadPatchJson().isBlank()) {
            try {
                jsonMapper.readTree(request.payloadPatchJson());
            } catch (JacksonException ex) {
                throw new ValidationDomainException("INVALID_PATCH_JSON", "Invalid RFC 7396 merge patch JSON", ex);
            }
        }

        int snapshotCount = messageRepository.countEligibleForPlan(
                request.dltTopicFilter(), request.fingerprintFilter(), maxRecords);
        String specHash = makerCheckerPolicy.computePlanSpecHash(
                request.name().trim(),
                request.fingerprintFilter(),
                request.dltTopicFilter(),
                request.targetTopic().trim(),
                orderingMode,
                request.payloadPatchJson(),
                rateLimit,
                maxRecords,
                snapshotCount);

        boolean requiresApproval = properties.getReplay().isRequireFourEyesApproval()
                || actor.role() == io.github.duhanguler.dltdeck.core.context.ActorRole.MCP_AGENT;
        ReplayPlanStatus initialStatus =
                requiresApproval ? ReplayPlanStatus.PENDING_APPROVAL : ReplayPlanStatus.APPROVED;

        UUID planId = UUID.randomUUID();
        Instant now = Instant.now(clock);
        ReplayPlan plan = new ReplayPlan(
                planId,
                request.name().trim(),
                request.fingerprintFilter(),
                request.dltTopicFilter(),
                request.targetTopic().trim(),
                orderingMode,
                request.payloadPatchJson(),
                rateLimit,
                maxRecords,
                snapshotCount,
                specHash,
                initialStatus,
                requiresApproval,
                actor.actorId(),
                requiresApproval ? null : actor.actorId(),
                null,
                0,
                0,
                0,
                0,
                0,
                0,
                now,
                requiresApproval ? null : now,
                null);
        planRepository.save(plan);
        audit(
                "DRAFT_REPLAY_PLAN",
                "REPLAY_PLAN",
                planId.toString(),
                Map.of(
                        "targetTopic", plan.targetTopic(),
                        "orderingMode", plan.orderingMode().name(),
                        "snapshotCount", snapshotCount,
                        "planSpecHash", specHash,
                        "requiresApproval", requiresApproval));
        return plan;
    }

    public ReplayPlan approveReplayPlan(UUID planId, @Nullable String expectedSpecHash, boolean executeAsync) {
        ActorContext approver = ActorContext.currentOrSystem();
        ReplayPlan plan = planRepository
                .findById(planId)
                .orElseThrow(() -> new NotFoundException("PLAN_NOT_FOUND", "Replay plan not found: " + planId));

        makerCheckerPolicy.verifyApprovalAllowed(plan, approver, expectedSpecHash);
        boolean approved = planRepository.approve(planId, approver.actorId(), Instant.now(clock));
        if (!approved) {
            throw new PlanConflictException("PLAN_APPROVAL_RACE", "Replay plan state changed concurrently");
        }

        audit(
                "APPROVE_REPLAY_PLAN",
                "REPLAY_PLAN",
                planId.toString(),
                Map.of("planSpecHash", plan.planSpecHash(), "createdBy", plan.createdBy()));

        ReplayPlan updated = planRepository.findById(planId).orElseThrow();
        if (executeAsync) {
            var _ = asyncPlanRunner.submit(() -> replayExecutor.executePlan(planId, properties.getPodId()));
            return updated;
        }
        return replayExecutor.executePlan(planId, properties.getPodId());
    }

    public ReplayPlan rejectReplayPlan(UUID planId, String reason) {
        ActorContext approver = ActorContext.currentOrSystem();
        if (!approver.role().canApprove()) {
            throw new ForbiddenActionException("APPROVER_ROLE_REQUIRED", "Only APPROVER actors can reject replay plans");
        }
        ReplayPlan plan = planRepository
                .findById(planId)
                .orElseThrow(() -> new NotFoundException("PLAN_NOT_FOUND", "Replay plan not found: " + planId));
        if (!planRepository.reject(planId, approver.actorId(), reason, Instant.now(clock))) {
            throw new PlanConflictException("PLAN_NOT_PENDING_APPROVAL", "Plan is not pending approval: " + plan.status());
        }
        audit("REJECT_REPLAY_PLAN", "REPLAY_PLAN", planId.toString(), Map.of("reason", reason));
        return planRepository.findById(planId).orElseThrow();
    }

    public ReplayPlan executeApprovedPlan(UUID planId) {
        requireOperator();
        return replayExecutor.executePlan(planId, properties.getPodId());
    }

    public List<ReplayPlan> listPlans(int limit) {
        return planRepository.listRecent(limit);
    }

    public ReplayPlan getPlan(UUID planId) {
        return planRepository
                .findById(planId)
                .orElseThrow(() -> new NotFoundException("PLAN_NOT_FOUND", "Replay plan not found: " + planId));
    }

    public void discardMessage(UUID messageId, String reason) {
        requireOperator();
        if (!messageRepository.discardMessage(messageId, Instant.now(clock))) {
            throw new PlanConflictException("DISCARD_NOT_ALLOWED", "Message cannot be discarded in its current status");
        }
        audit("DISCARD_MESSAGE", "MESSAGE", messageId.toString(), Map.of("reason", reason));
    }

    public void resolveIndeterminate(UUID messageId, MessageStatus targetStatus, String justification) {
        ActorContext actor = ActorContext.currentOrSystem();
        if (!actor.role().canApprove()) {
            throw new ForbiddenActionException(
                    "APPROVER_ROLE_REQUIRED", "Resolving INDETERMINATE/BLOCKED/QUARANTINED records requires APPROVER role");
        }
        if (!messageRepository.resolveIndeterminate(messageId, targetStatus, Instant.now(clock))) {
            throw new PlanConflictException(
                    "RESOLUTION_NOT_ALLOWED", "Message is not in INDETERMINATE, BLOCKED_BY_PRIOR_KEY_FAILURE, or QUARANTINED state");
        }
        audit(
                "RESOLVE_MANUAL_STATE",
                "MESSAGE",
                messageId.toString(),
                Map.of("targetStatus", targetStatus.name(), "justification", justification));
    }

    public String exportMessagesCsv(@Nullable String dltTopic, @Nullable String fingerprint, int limit) {
        KeysetPage<DltMessageSummary> page = messageRepository.listMessages(dltTopic, fingerprint, null, null, limit);
        StringBuilder csv = new StringBuilder(1024);
        csv.append("id,dlt_topic,dlt_partition,dlt_offset,original_topic,status,failure_category,payload_format,replay_hops,ingested_at\n");
        for (DltMessageSummary m : page.items()) {
            csv.append(CsvExportSanitizer.sanitizeCell(m.id().toString())).append(',')
                    .append(CsvExportSanitizer.sanitizeCell(m.dltTopic())).append(',')
                    .append(m.dltPartition()).append(',')
                    .append(m.dltOffset()).append(',')
                    .append(CsvExportSanitizer.sanitizeCell(m.originalTopic())).append(',')
                    .append(m.status().name()).append(',')
                    .append(m.failureCategory().name()).append(',')
                    .append(m.payloadFormat().name()).append(',')
                    .append(m.replayHops()).append(',')
                    .append(m.ingestedAt()).append('\n');
        }
        audit("EXPORT_CSV", "TOPIC", dltTopic == null ? "*" : dltTopic, Map.of("rowCount", page.items().size()));
        return csv.toString();
    }

    public List<AuditEntry> listAuditEntries(int limit) {
        return auditRepository.listRecent(limit);
    }

    public AuditChainVerification verifyAuditChain() {
        return auditRepository.verifyIntegrity();
    }

    private void requireOperator() {
        ActorContext actor = ActorContext.currentOrSystem();
        if (!actor.role().canOperate()) {
            throw new ForbiddenActionException(
                    "OPERATOR_ROLE_REQUIRED", "Actor '" + actor.actorId() + "' lacks OPERATOR privileges");
        }
    }

    private void audit(String action, String targetType, String targetId, Map<String, Object> details) {
        ActorContext actor = ActorContext.currentOrSystem();
        String detailsJson;
        try {
            detailsJson = piiRedactor.redactText(jsonMapper.writeValueAsString(details));
        } catch (JacksonException ex) {
            detailsJson = "{}";
        }
        auditRepository.append(
                UUID.randomUUID(),
                actor.actorId(),
                actor.role().name(),
                action,
                targetType,
                targetId,
                null,
                detailsJson,
                Instant.now(clock));
    }

    public record OverviewSnapshot(
            String podId,
            String actorId,
            String actorRole,
            boolean ingestionRunning,
            long totalIngestionLag,
            Map<String, Long> ingestionLagByTopic,
            Set<String> subscribedTopics,
            int clusterCount,
            long totalMessages,
            long actionableMessages,
            int indeterminateCount,
            int blockedHolCount,
            long pendingApprovalPlans,
            long pausedBlastRadiusPlans,
            Instant generatedAt) {}

    public record InspectedMessage(
            DltMessageSummary summary,
            @Nullable String exceptionFqcn,
            @Nullable String rootCauseFqcn,
            @Nullable String sampleMessage,
            @Nullable String canonicalStacktrace,
            String recommendation,
            io.github.duhanguler.dltdeck.core.model.PayloadFormat payloadFormat,
            boolean patchable,
            @Nullable Integer schemaId,
            @Nullable String messageKey,
            String payloadPreview,
            Map<String, String> headers,
            @Nullable String cloudEventType,
            @Nullable String cloudEventId,
            @Nullable String cloudEventSource,
            boolean piiRedacted) {}

    public record DraftPlanRequest(
            String name,
            @Nullable String fingerprintFilter,
            @Nullable String dltTopicFilter,
            String targetTopic,
            @Nullable ReplayOrderingMode orderingMode,
            @Nullable String payloadPatchJson,
            @Nullable Integer rateLimitPerSec,
            @Nullable Integer maxRecords) {}
}
