package io.github.duhanguler.dltdeck.mcp;

import io.github.duhanguler.dltdeck.core.context.ActorContext;
import io.github.duhanguler.dltdeck.core.context.ActorRole;
import io.github.duhanguler.dltdeck.core.model.DryRunOutcome;
import io.github.duhanguler.dltdeck.core.model.DryRunTier;
import io.github.duhanguler.dltdeck.core.model.FingerprintCluster;
import io.github.duhanguler.dltdeck.core.model.ReplayOrderingMode;
import io.github.duhanguler.dltdeck.core.model.ReplayPlan;
import io.github.duhanguler.dltdeck.starter.service.DltDeckControlPlaneService;
import java.util.List;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Fail-closed Human-in-the-Loop (HITL) MCP service surface.
 *
 * <p>Security boundary enforced both by Java API design and {@code archunit-junit6}:
 * <ul>
 *   <li>All calls execute under {@link ActorRole#MCP_AGENT} via Java 25 {@link ScopedValue}.</li>
 *   <li>{@link #inspectDltMessage(UUID)} ALWAYS enforces PII redaction ({@code unmaskPii = false}).</li>
 *   <li>{@link #dryRunDltPatch(UUID, String)} ONLY permits Tier-1 side-effect-free validation.</li>
 *   <li>{@link #draftReplayPlan} ALWAYS creates plans in {@code PENDING_APPROVAL} requiring a human {@code APPROVER}.</li>
 *   <li>Zero code path exists to approve, reject, execute, discard, or unmask PII.</li>
 * </ul>
 */
public final class DltDeckMcpService {

    private final DltDeckControlPlaneService controlPlane;

    public DltDeckMcpService(DltDeckControlPlaneService controlPlane) {
        this.controlPlane = controlPlane;
    }

    public List<FingerprintCluster> listDltClusters(@Nullable String dltTopic, int limit) {
        return runAsMcp(() -> controlPlane.listClusters(dltTopic, Math.clamp(limit, 1, 50)));
    }

    public DltDeckControlPlaneService.InspectedMessage inspectDltMessage(UUID messageId) {
        // Hard-coded unmaskPii=false: MCP agents can NEVER read raw unmasked PII
        return runAsMcp(() -> controlPlane.inspectMessage(messageId, false));
    }

    public DryRunOutcome dryRunDltPatch(UUID messageId, @Nullable String mergePatchJson) {
        // Hard-coded TIER_1_VALIDATE: MCP agents can NEVER trigger Tier-3 canary writes
        return runAsMcp(() -> controlPlane.dryRun(messageId, DryRunTier.TIER_1_VALIDATE, null, mergePatchJson));
    }

    public ReplayPlan draftReplayPlan(
            String name,
            @Nullable String dltTopicFilter,
            @Nullable String fingerprintFilter,
            String targetTopic,
            @Nullable String payloadPatchJson,
            int rateLimitPerSec,
            int maxRecords) {
        return runAsMcp(() -> controlPlane.draftReplayPlan(new DltDeckControlPlaneService.DraftPlanRequest(
                name,
                fingerprintFilter,
                dltTopicFilter,
                targetTopic,
                ReplayOrderingMode.PRESERVE_KEY_ORDER,
                payloadPatchJson,
                Math.clamp(rateLimitPerSec, 1, 200),
                Math.clamp(maxRecords, 1, 1000))));
    }

    private <T> T runAsMcp(java.util.function.Supplier<T> supplier) {
        ActorContext mcpActor = new ActorContext("mcp-agent", ActorRole.MCP_AGENT, UUID.randomUUID().toString());
        return ScopedValue.where(ActorContext.CURRENT, mcpActor).call(supplier::get);
    }
}
