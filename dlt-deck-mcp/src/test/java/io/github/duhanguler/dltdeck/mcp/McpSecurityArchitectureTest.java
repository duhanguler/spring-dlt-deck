package io.github.duhanguler.dltdeck.mcp;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import java.util.Set;

/**
 * Build-time architectural proof that {@code dlt-deck-mcp} is strictly fail-closed HITL:
 * zero code path may approve, reject, execute, discard, or manually resolve DLT replay records.
 */
@AnalyzeClasses(
        packages = "io.github.duhanguler.dltdeck.mcp",
        importOptions = ImportOption.DoNotIncludeTests.class)
class McpSecurityArchitectureTest {

    private static final Set<String> FORBIDDEN_CONTROL_PLANE_METHODS = Set.of(
            "approveReplayPlan",
            "rejectReplayPlan",
            "executeApprovedPlan",
            "executePlan",
            "resolveIndeterminate",
            "discardMessage");

    @ArchTest
    static final ArchRule mcpModuleCannotInvokePrivilegedApprovalOrMutationPaths = noClasses()
            .should(new ArchCondition<>("call privileged approval/execution/resolution methods") {
                @Override
                public void check(com.tngtech.archunit.core.domain.JavaClass item, ConditionEvents events) {
                    for (JavaMethodCall call : item.getMethodCallsFromSelf()) {
                        if (FORBIDDEN_CONTROL_PLANE_METHODS.contains(call.getTarget().getName())) {
                            events.add(SimpleConditionEvent.violated(
                                    call,
                                    item.getFullName() + " illegally calls " + call.getTarget().getFullName()));
                        }
                    }
                }
            });

    @ArchTest
    static final ArchRule mcpNeverUsesThreadLocal = noClasses()
            .should()
            .dependOnClassesThat()
            .haveFullyQualifiedName("java.lang.ThreadLocal");
}
