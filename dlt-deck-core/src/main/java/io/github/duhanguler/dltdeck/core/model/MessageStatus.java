package io.github.duhanguler.dltdeck.core.model;

public enum MessageStatus {
    NEW,
    CLAIMED,
    PUBLISHED,
    FAILED_PUBLISH,
    INDETERMINATE,
    RE_DLT_DETECTED,
    BLOCKED_BY_PRIOR_KEY_FAILURE,
    DISCARDED,
    QUARANTINED;

    public boolean isReplayEligible() {
        return this == NEW || this == FAILED_PUBLISH;
    }

    public boolean requiresManualIntervention() {
        return this == INDETERMINATE || this == BLOCKED_BY_PRIOR_KEY_FAILURE || this == QUARANTINED;
    }

    public boolean isTerminal() {
        return this == PUBLISHED || this == DISCARDED;
    }
}
