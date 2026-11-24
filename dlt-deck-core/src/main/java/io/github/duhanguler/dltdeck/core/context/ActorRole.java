package io.github.duhanguler.dltdeck.core.context;

public enum ActorRole {
    VIEWER(1),
    MCP_AGENT(1),
    OPERATOR(2),
    APPROVER(3),
    SYSTEM(4);

    private final int privilegeLevel;

    ActorRole(int privilegeLevel) {
        this.privilegeLevel = privilegeLevel;
    }

    public boolean canView() {
        return true;
    }

    public boolean canOperate() {
        return this == OPERATOR || this == APPROVER || this == SYSTEM;
    }

    public boolean canApprove() {
        return this == APPROVER || this == SYSTEM;
    }

    public boolean canUnmaskPii() {
        return this == APPROVER || this == SYSTEM;
    }

    public int privilegeLevel() {
        return privilegeLevel;
    }
}
