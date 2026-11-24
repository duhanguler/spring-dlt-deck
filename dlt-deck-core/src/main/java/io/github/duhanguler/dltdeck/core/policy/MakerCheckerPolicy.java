package io.github.duhanguler.dltdeck.core.policy;

import io.github.duhanguler.dltdeck.core.context.ActorContext;
import io.github.duhanguler.dltdeck.core.exception.ForbiddenActionException;
import io.github.duhanguler.dltdeck.core.exception.PlanConflictException;
import io.github.duhanguler.dltdeck.core.fingerprint.ExceptionFingerprinter;
import io.github.duhanguler.dltdeck.core.model.ReplayOrderingMode;
import io.github.duhanguler.dltdeck.core.model.ReplayPlan;
import io.github.duhanguler.dltdeck.core.model.ReplayPlanStatus;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Locale;
import org.jspecify.annotations.Nullable;

/**
 * Four-eyes (maker != checker) approval policy with immutable {@code planSpecHash} TOCTOU defense.
 */
public final class MakerCheckerPolicy {

    public String computePlanSpecHash(
            String name,
            @Nullable String fingerprintFilter,
            @Nullable String dltTopicFilter,
            String targetTopic,
            ReplayOrderingMode orderingMode,
            @Nullable String payloadPatchJson,
            int rateLimitPerSec,
            int maxRecords,
            int snapshotCount) {
        StringBuilder canonical = new StringBuilder(256);
        appendField(canonical, name);
        appendField(canonical, fingerprintFilter);
        appendField(canonical, dltTopicFilter);
        appendField(canonical, targetTopic);
        appendField(canonical, orderingMode.name());
        appendField(canonical, payloadPatchJson);
        appendField(canonical, Integer.toString(rateLimitPerSec));
        appendField(canonical, Integer.toString(maxRecords));
        appendField(canonical, Integer.toString(snapshotCount));
        return ExceptionFingerprinter.sha256Hex(canonical.toString());
    }

    public String computePlanSpecHash(ReplayPlan plan) {
        return computePlanSpecHash(
                plan.name(),
                plan.fingerprintFilter(),
                plan.dltTopicFilter(),
                plan.targetTopic(),
                plan.orderingMode(),
                plan.payloadPatchJson(),
                plan.rateLimitPerSec(),
                plan.maxRecords(),
                plan.snapshotCount());
    }

    public void verifyApprovalAllowed(ReplayPlan plan, ActorContext approver, @Nullable String expectedSpecHash) {
        if (!approver.role().canApprove()) {
            throw new ForbiddenActionException(
                    "APPROVER_ROLE_REQUIRED",
                    "Actor '" + approver.actorId() + "' with role " + approver.role() + " cannot approve replay plans");
        }
        if (plan.createdBy().toLowerCase(Locale.ROOT).equals(approver.actorId().toLowerCase(Locale.ROOT))) {
            throw new ForbiddenActionException(
                    "SELF_APPROVAL_FORBIDDEN",
                    "Four-eyes policy violation: creator '" + plan.createdBy() + "' cannot self-approve replay plan "
                            + plan.id());
        }
        if (plan.status() != ReplayPlanStatus.PENDING_APPROVAL) {
            throw new PlanConflictException(
                    "PLAN_NOT_PENDING_APPROVAL",
                    "Replay plan " + plan.id() + " is in status " + plan.status() + " and cannot be approved");
        }

        String actualHash = computePlanSpecHash(plan);
        if (!constantTimeEquals(actualHash, plan.planSpecHash())) {
            throw new PlanConflictException(
                    "PLAN_SPEC_HASH_MISMATCH",
                    "TOCTOU integrity violation: persisted plan parameters do not match frozen planSpecHash");
        }
        if (expectedSpecHash != null && !expectedSpecHash.isBlank() && !constantTimeEquals(actualHash, expectedSpecHash)) {
            throw new PlanConflictException(
                    "EXPECTED_SPEC_HASH_MISMATCH",
                    "TOCTOU integrity violation: expected planSpecHash does not match current frozen plan");
        }
    }

    public void verifyExecutionIntegrity(ReplayPlan plan) {
        if (!plan.status().isExecutable()) {
            throw new PlanConflictException(
                    "PLAN_NOT_EXECUTABLE",
                    "Replay plan " + plan.id() + " is in status " + plan.status() + " and cannot execute");
        }
        if (plan.requiresApproval()) {
            if (plan.approvedBy() == null || plan.approvedBy().isBlank()) {
                throw new ForbiddenActionException(
                        "APPROVAL_REQUIRED", "Replay plan " + plan.id() + " requires independent approval");
            }
            if (plan.createdBy().toLowerCase(Locale.ROOT).equals(plan.approvedBy().toLowerCase(Locale.ROOT))) {
                throw new ForbiddenActionException(
                        "SELF_APPROVAL_FORBIDDEN", "Replay plan " + plan.id() + " was self-approved");
            }
        }
        String actualHash = computePlanSpecHash(plan);
        if (!constantTimeEquals(actualHash, plan.planSpecHash())) {
            throw new PlanConflictException(
                    "PLAN_SPEC_HASH_MISMATCH",
                    "Execution aborted: replay plan " + plan.id() + " mutated after approval freeze");
        }
    }

    private static void appendField(StringBuilder sb, @Nullable String value) {
        if (value == null) {
            sb.append("-1:|");
        } else {
            sb.append(value.length()).append(':').append(value).append('|');
        }
    }

    private static boolean constantTimeEquals(String a, String b) {
        return MessageDigest.isEqual(
                a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }
}
