package io.github.duhanguler.dltdeck.core.model;

public enum ReplayPlanStatus {
    DRAFT,
    PENDING_APPROVAL,
    APPROVED,
    RUNNING,
    PAUSED_BLAST_RADIUS,
    COMPLETED,
    COMPLETED_WITH_FAILURES,
    REJECTED,
    CANCELLED;

    public boolean isExecutable() {
        return this == APPROVED || this == RUNNING;
    }

    public boolean isTerminal() {
        return this == COMPLETED || this == COMPLETED_WITH_FAILURES || this == REJECTED || this == CANCELLED;
    }
}
