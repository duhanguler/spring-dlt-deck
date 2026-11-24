package io.github.duhanguler.dltdeck.starter.aot;

import io.github.duhanguler.dltdeck.core.model.AuditChainVerification;
import io.github.duhanguler.dltdeck.core.model.AuditEntry;
import io.github.duhanguler.dltdeck.core.model.DltMessageSummary;
import io.github.duhanguler.dltdeck.core.model.DryRunOutcome;
import io.github.duhanguler.dltdeck.core.model.FingerprintCluster;
import io.github.duhanguler.dltdeck.core.model.KeysetPage;
import io.github.duhanguler.dltdeck.core.model.ReplayPlan;
import io.github.duhanguler.dltdeck.starter.service.DltDeckControlPlaneService;
import io.github.duhanguler.dltdeck.starter.web.DltDeckWebHandler;
import org.jspecify.annotations.Nullable;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;

/**
 * Spring AOT / GraalVM Native Image runtime hints for SQL migration scripts, Lua rate-limiter scripts,
 * embedded Workbench UI assets, and private Jackson 3 DTO records.
 */
public final class DltDeckRuntimeHints implements RuntimeHintsRegistrar {

    @Override
    public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {
        hints.resources().registerPattern("dltdeck/sql/*/*.sql");
        hints.resources().registerPattern("dltdeck/redis/*.lua");
        hints.resources().registerPattern("dltdeck/ui/*");

        Class<?>[] jsonTypes = {
            DltMessageSummary.class,
            FingerprintCluster.class,
            KeysetPage.class,
            ReplayPlan.class,
            DryRunOutcome.class,
            AuditEntry.class,
            AuditChainVerification.class,
            DltDeckControlPlaneService.OverviewSnapshot.class,
            DltDeckControlPlaneService.InspectedMessage.class,
            DltDeckControlPlaneService.DraftPlanRequest.class,
            DltDeckWebHandler.PatchPreviewBody.class,
            DltDeckWebHandler.DryRunBody.class,
            DltDeckWebHandler.ReasonBody.class,
            DltDeckWebHandler.ResolveBody.class,
            DltDeckWebHandler.DraftPlanBody.class,
            DltDeckWebHandler.ApprovePlanBody.class
        };
        for (Class<?> type : jsonTypes) {
            hints.reflection().registerType(type, MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS, MemberCategory.INVOKE_PUBLIC_METHODS);
        }
    }
}
