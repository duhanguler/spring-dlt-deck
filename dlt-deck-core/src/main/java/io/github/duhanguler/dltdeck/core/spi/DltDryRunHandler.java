package io.github.duhanguler.dltdeck.core.spi;

import io.github.duhanguler.dltdeck.core.model.DltMessageDetail;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Explicit opt-in Tier-2 Dry-Run SPI implemented by host applications to run side-effect-free
 * domain validation against candidate replay payloads.
 */
public interface DltDryRunHandler {

    boolean supports(String targetTopic);

    HandlerResult dryRun(String targetTopic, DltMessageDetail message, byte @Nullable [] candidatePayload);

    record HandlerResult(boolean passed, String summary, List<String> diagnostics) {
        public HandlerResult {
            diagnostics = List.copyOf(diagnostics);
        }
    }
}
