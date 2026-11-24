package io.github.duhanguler.dltdeck.core.model;

import java.util.List;
import org.jspecify.annotations.Nullable;

public record DryRunOutcome(
        DryRunTier tier,
        boolean passed,
        String summary,
        List<String> diagnostics,
        @Nullable String canaryTopic,
        @Nullable Integer canaryPartition,
        @Nullable Long canaryOffset,
        long durationMs) {

    public DryRunOutcome {
        diagnostics = List.copyOf(diagnostics);
    }
}
