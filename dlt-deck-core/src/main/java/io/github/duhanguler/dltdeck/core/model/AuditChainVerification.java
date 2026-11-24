package io.github.duhanguler.dltdeck.core.model;

import org.jspecify.annotations.Nullable;

public record AuditChainVerification(
        boolean valid,
        long totalChecked,
        @Nullable Long firstInvalidSequenceNo,
        @Nullable String failureReason,
        @Nullable String latestHash) {

    public static AuditChainVerification intact(long totalChecked, @Nullable String latestHash) {
        return new AuditChainVerification(true, totalChecked, null, null, latestHash);
    }

    public static AuditChainVerification broken(long totalChecked, long sequenceNo, String reason) {
        return new AuditChainVerification(false, totalChecked, sequenceNo, reason, null);
    }
}
